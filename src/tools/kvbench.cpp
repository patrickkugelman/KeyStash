// closed-loop load generator for RESP servers: ops/sec and latency percentiles
//
// every client thread has one connection and loops: send `pipeline` commands,
// wait for all the replies. latency is that round trip (per command when
// pipeline is 1). clients slow down when the server does, so tail latency
// under overload looks better than it really is

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Config {
  std::string host = "127.0.0.1";
  uint16_t port = 6379;
  int clients = 50;
  uint64_t requests = 1000000;  // total over all clients
  int pipeline = 1;
  size_t value_size = 64;
  uint64_t keyspace = 100000;
  std::string mode = "mixed";   // set | get | incr | mixed
  int set_percent = 50;         // sets in mixed mode
  bool preload = false;         // fill the keyspace first, not timed
  std::string side_command;     // sent once mid-run on its own connection
  int side_after_ms = 0;
  uint64_t seed = 1;
};

const char* const kUsage =
    "usage: kvbench [flags]\n"
    "  --host H          server address (default 127.0.0.1)\n"
    "  --port N          server port (default 6379)\n"
    "  --clients N       concurrent connections (default 50)\n"
    "  --requests N      total commands (default 1000000)\n"
    "  --pipeline N      commands per round trip, 1..1024 (default 1)\n"
    "  --value-size N    bytes per value (default 64)\n"
    "  --keyspace N      distinct keys (default 100000)\n"
    "  --mode M          set | get | incr | mixed (default mixed)\n"
    "  --set-percent N   share of SETs in mixed mode (default 50)\n"
    "  --preload         SET every key once before the timed run\n"
    "  --side-command C  send C (inline, e.g. BGSAVE) once during the run\n"
    "  --side-after-ms N delay before the side command (default 0)\n"
    "  --seed N          random seed (default 1)\n";

bool parse_u64(std::string_view s, uint64_t* out) {
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(s.data(), end, *out);
  return ec == std::errc() && ptr == end && !s.empty();
}

int connect_to(const Config& cfg) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(cfg.port);
  if (inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr) != 1 ||
      ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return -1;
  }
  const int on = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
  return fd;
}

bool send_all(int fd, const char* p, size_t n) {
  while (n > 0) {
    const ssize_t w = ::send(fd, p, n, MSG_NOSIGNAL);
    if (w < 0) return false;
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

// length of the reply at p: 0 = need more bytes, -1 = garbage
// only handles the reply types these commands give back
ptrdiff_t reply_length(const char* p, size_t n) {
  if (n == 0) return 0;
  const void* nl = std::memchr(p, '\n', n);
  if (nl == nullptr) return 0;
  const size_t line = static_cast<size_t>(static_cast<const char*>(nl) - p) + 1;
  switch (p[0]) {
    case '+':
    case '-':
    case ':':
      return static_cast<ptrdiff_t>(line);
    case '$': {
      int64_t len = 0;
      auto [ptr, ec] = std::from_chars(p + 1, p + line - 2, len);
      if (ec != std::errc() || ptr != p + line - 2) return -1;
      if (len < 0) return static_cast<ptrdiff_t>(line);
      const size_t total = line + static_cast<size_t>(len) + 2;
      return n >= total ? static_cast<ptrdiff_t>(total) : 0;
    }
    default:
      return -1;
  }
}

struct Result {
  std::vector<uint32_t> batch_us;  // per round trip
  uint64_t commands = 0;
  uint64_t errors = 0;     // error replies and dead connections
  uint64_t get_hits = 0;
  uint64_t gets = 0;
  bool connect_failed = false;
};

struct Rng {
  uint64_t s;
  uint64_t next() {  // xorshift64*
    s ^= s >> 12;
    s ^= s << 25;
    s ^= s >> 27;
    return s * 2685821657736338717ull;
  }
};

void append_command(std::string& out, const char* name, const std::string& key,
                    const std::string* value) {
  const size_t argc = value != nullptr ? 3 : 2;
  out += "*" + std::to_string(argc) + "\r\n$" + std::to_string(std::strlen(name)) + "\r\n" + name +
         "\r\n$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
  if (value != nullptr) out += "$" + std::to_string(value->size()) + "\r\n" + *value + "\r\n";
}

std::string make_key(uint64_t n) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key:%010llu", static_cast<unsigned long long>(n));
  return buf;
}

// reads count replies, false if the connection died or sent garbage
bool read_replies(int fd, std::vector<char>& buf, size_t& pos, size_t& end, int count,
                  Result& r, const std::vector<char>& is_get) {
  for (int i = 0; i < count; ++i) {
    ptrdiff_t len;
    while ((len = reply_length(buf.data() + pos, end - pos)) == 0) {
      if (pos > 0) {
        std::memmove(buf.data(), buf.data() + pos, end - pos);
        end -= pos;
        pos = 0;
      }
      if (end == buf.size()) buf.resize(buf.size() * 2);
      const ssize_t n = ::recv(fd, buf.data() + end, buf.size() - end, 0);
      if (n <= 0) return false;
      end += static_cast<size_t>(n);
    }
    if (len < 0) return false;
    const char first = buf[pos];
    if (first == '-') ++r.errors;
    if (is_get[static_cast<size_t>(i)]) {
      ++r.gets;
      if (first == '$' && buf[pos + 1] != '-') ++r.get_hits;
    }
    pos += static_cast<size_t>(len);
  }
  return true;
}

void run_client(const Config& cfg, int index, uint64_t share, std::atomic<int>& ready,
                std::mutex& mu, std::condition_variable& go_cv, bool& go, Result& r) {
  const int fd = connect_to(cfg);
  if (fd < 0) r.connect_failed = true;
  {
    std::unique_lock<std::mutex> lock(mu);
    ready.fetch_add(1);
    go_cv.wait(lock, [&] { return go; });
  }
  if (fd < 0) return;

  Rng rng{cfg.seed * 0x9E3779B97F4A7C15ull + static_cast<uint64_t>(index) + 1};
  const std::string value(cfg.value_size, 'x');
  std::vector<char> buf(64 * 1024);
  size_t pos = 0, end = 0;
  std::string req;
  std::vector<char> is_get;
  r.batch_us.reserve(share / static_cast<uint64_t>(cfg.pipeline) + 1);

  for (uint64_t done = 0; done < share;) {
    const int batch = static_cast<int>(std::min<uint64_t>(cfg.pipeline, share - done));
    req.clear();
    is_get.assign(static_cast<size_t>(batch), 0);
    for (int i = 0; i < batch; ++i) {
      const std::string key = make_key(rng.next() % cfg.keyspace);
      bool set = cfg.mode == "set";
      if (cfg.mode == "mixed") set = static_cast<int>(rng.next() % 100) < cfg.set_percent;
      if (cfg.mode == "incr") {
        append_command(req, "INCR", key, nullptr);
      } else if (set) {
        append_command(req, "SET", key, &value);
      } else {
        append_command(req, "GET", key, nullptr);
        is_get[static_cast<size_t>(i)] = 1;
      }
    }
    const auto t0 = Clock::now();
    if (!send_all(fd, req.data(), req.size()) ||
        !read_replies(fd, buf, pos, end, batch, r, is_get)) {
      ++r.errors;
      break;
    }
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
    r.batch_us.push_back(static_cast<uint32_t>(std::min<int64_t>(us, UINT32_MAX)));
    r.commands += static_cast<uint64_t>(batch);
    done += static_cast<uint64_t>(batch);
  }
  ::close(fd);
}

void preload(const Config& cfg) {
  const int threads = std::min<int>(cfg.clients, 16);
  std::vector<std::thread> pool;
  for (int t = 0; t < threads; ++t) {
    pool.emplace_back([&, t] {
      const int fd = connect_to(cfg);
      if (fd < 0) return;
      const std::string value(cfg.value_size, 'x');
      std::vector<char> buf(64 * 1024);
      size_t pos = 0, end = 0;
      Result scratch;
      std::string req;
      const uint64_t lo = cfg.keyspace * static_cast<uint64_t>(t) / static_cast<uint64_t>(threads);
      const uint64_t hi = cfg.keyspace * static_cast<uint64_t>(t + 1) / static_cast<uint64_t>(threads);
      for (uint64_t k = lo; k < hi;) {
        const int batch = static_cast<int>(std::min<uint64_t>(256, hi - k));
        req.clear();
        for (int i = 0; i < batch; ++i) append_command(req, "SET", make_key(k + static_cast<uint64_t>(i)), &value);
        const std::vector<char> none(static_cast<size_t>(batch), 0);
        if (!send_all(fd, req.data(), req.size()) ||
            !read_replies(fd, buf, pos, end, batch, scratch, none)) {
          break;
        }
        k += static_cast<uint64_t>(batch);
      }
      ::close(fd);
    });
  }
  for (auto& th : pool) th.join();
}

double percentile(const std::vector<uint32_t>& sorted, double p) {
  if (sorted.empty()) return 0;
  const size_t idx = std::min(sorted.size() - 1, static_cast<size_t>(p / 100.0 * static_cast<double>(sorted.size())));
  return static_cast<double>(sorted[idx]);
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string_view flag = argv[i];
    if (flag == "--help" || flag == "-h") {
      std::fputs(kUsage, stdout);
      return 0;
    }
    if (flag == "--preload") {
      cfg.preload = true;
      continue;
    }
    if (i + 1 == argc) {
      std::fprintf(stderr, "missing value for %s\n%s", argv[i], kUsage);
      return 2;
    }
    const std::string_view value = argv[++i];
    uint64_t n = 0;
    bool ok = true;
    if (flag == "--host") cfg.host = std::string(value);
    else if (flag == "--mode") { cfg.mode = std::string(value); ok = cfg.mode == "set" || cfg.mode == "get" || cfg.mode == "incr" || cfg.mode == "mixed"; }
    else if (flag == "--side-command") cfg.side_command = std::string(value);
    else if (!parse_u64(value, &n)) ok = false;
    else if (flag == "--port") { ok = n >= 1 && n <= 65535; cfg.port = static_cast<uint16_t>(n); }
    else if (flag == "--clients") { ok = n >= 1 && n <= 10000; cfg.clients = static_cast<int>(n); }
    else if (flag == "--requests") { ok = n >= 1; cfg.requests = n; }
    else if (flag == "--pipeline") { ok = n >= 1 && n <= 1024; cfg.pipeline = static_cast<int>(n); }
    else if (flag == "--value-size") { ok = n <= (1u << 20); cfg.value_size = n; }
    else if (flag == "--keyspace") { ok = n >= 1; cfg.keyspace = n; }
    else if (flag == "--set-percent") { ok = n <= 100; cfg.set_percent = static_cast<int>(n); }
    else if (flag == "--side-after-ms") cfg.side_after_ms = static_cast<int>(n);
    else if (flag == "--seed") cfg.seed = n;
    else ok = false;
    if (!ok) {
      std::fprintf(stderr, "bad flag or value: %s %s\n%s", argv[i - 1], argv[i], kUsage);
      return 2;
    }
  }
  if (static_cast<uint64_t>(cfg.clients) > cfg.requests) cfg.clients = static_cast<int>(cfg.requests);

  if (cfg.preload) {
    const auto t0 = Clock::now();
    preload(cfg);
    std::printf("preloaded %llu keys in %.2f s\n", static_cast<unsigned long long>(cfg.keyspace),
                std::chrono::duration<double>(Clock::now() - t0).count());
  }

  std::vector<Result> results(static_cast<size_t>(cfg.clients));
  std::vector<std::thread> threads;
  std::atomic<int> ready{0};
  std::mutex mu;
  std::condition_variable go_cv;
  bool go = false;
  for (int c = 0; c < cfg.clients; ++c) {
    const uint64_t share = cfg.requests / static_cast<uint64_t>(cfg.clients) +
                           (static_cast<uint64_t>(c) < cfg.requests % static_cast<uint64_t>(cfg.clients) ? 1 : 0);
    threads.emplace_back(run_client, std::cref(cfg), c, share, std::ref(ready), std::ref(mu),
                         std::ref(go_cv), std::ref(go), std::ref(results[static_cast<size_t>(c)]));
  }
  while (ready.load() < cfg.clients) std::this_thread::sleep_for(std::chrono::milliseconds(1));

  const auto start = Clock::now();
  {
    std::lock_guard<std::mutex> lock(mu);
    go = true;
  }
  go_cv.notify_all();

  std::thread side;
  if (!cfg.side_command.empty()) {
    side = std::thread([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(cfg.side_after_ms));
      const int fd = connect_to(cfg);
      if (fd < 0) return;
      const std::string line = cfg.side_command + "\r\n";
      char reply[512];
      if (send_all(fd, line.data(), line.size())) {
        const ssize_t n = ::recv(fd, reply, sizeof(reply) - 1, 0);
        if (n > 0) {
          while (n > 0 && (reply[n - 1] == '\r' || reply[n - 1] == '\n')) --const_cast<ssize_t&>(n);
          std::printf("side command '%s' at +%d ms -> %.*s\n", cfg.side_command.c_str(), cfg.side_after_ms,
                      static_cast<int>(n), reply);
        }
      }
      ::close(fd);
    });
  }
  for (auto& t : threads) t.join();
  const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
  if (side.joinable()) side.join();

  std::vector<uint32_t> all;
  uint64_t commands = 0, errors = 0, gets = 0, hits = 0;
  bool connect_failed = false;
  for (const Result& r : results) {
    all.insert(all.end(), r.batch_us.begin(), r.batch_us.end());
    commands += r.commands;
    errors += r.errors;
    gets += r.gets;
    hits += r.get_hits;
    connect_failed = connect_failed || r.connect_failed;
  }
  if (connect_failed) std::fprintf(stderr, "warning: some clients could not connect\n");
  std::sort(all.begin(), all.end());
  const uint64_t slow = static_cast<uint64_t>(std::count_if(all.begin(), all.end(), [](uint32_t us) { return us > 10000; }));

  std::printf("clients=%d pipeline=%d mode=%s%s value=%zuB keyspace=%llu (closed loop)\n", cfg.clients,
              cfg.pipeline, cfg.mode.c_str(),
              cfg.mode == "mixed" ? (" set=" + std::to_string(cfg.set_percent) + "%").c_str() : "",
              cfg.value_size, static_cast<unsigned long long>(cfg.keyspace));
  std::printf("commands: %llu in %.3f s\n", static_cast<unsigned long long>(commands), seconds);
  std::printf("throughput: %.0f ops/sec\n", static_cast<double>(commands) / seconds);
  std::printf("latency per round trip (us): p50 %.0f  p99 %.0f  p99.9 %.0f  max %.0f  (%zu round trips)\n",
              percentile(all, 50), percentile(all, 99), percentile(all, 99.9),
              all.empty() ? 0.0 : static_cast<double>(all.back()), all.size());
  std::printf("round trips over 10 ms: %llu\n", static_cast<unsigned long long>(slow));
  if (gets > 0) std::printf("get hit rate: %.1f%%\n", 100.0 * static_cast<double>(hits) / static_cast<double>(gets));
  std::printf("errors: %llu\n", static_cast<unsigned long long>(errors));
  return errors == 0 && !connect_failed ? 0 : 1;
}
