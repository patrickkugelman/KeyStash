#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <charconv>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "engine/dispatcher.h"
#include "engine/hash_table.h"
#include "engine/std_table.h"
#include "net/event_loop.h"
#include "net/server.h"
#include "persist/persistence.h"

namespace {

// SIGINT/SIGTERM stop the loop
class SignalWatcher : public net::EventHandler {
 public:
  explicit SignalWatcher(net::EventLoop& loop) : loop_(loop) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &mask, nullptr) != 0) throw std::runtime_error("sigprocmask failed");
    fd_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (fd_ < 0) throw std::runtime_error("signalfd failed");
    if (!loop_.add(fd_, EPOLLIN, this)) throw std::runtime_error("epoll add signalfd failed");
  }
  ~SignalWatcher() override { ::close(fd_); }

  void on_event(uint32_t) override { loop_.stop(); }

 private:
  net::EventLoop& loop_;
  int fd_ = -1;
};

struct Options {
  std::string bind = "127.0.0.1";
  uint16_t port = 6379;
  std::string table = "custom";  // custom | std
  std::string dir;               // empty = no persistence
  persist::FsyncPolicy fsync = persist::FsyncPolicy::kEverySec;
  uint64_t snapshot_wal_bytes = 64ull << 20;  // 0 = no automatic snapshots
  bool help = false;
  bool persistence_flag_given = false;  // --fsync or --snapshot-wal-bytes was passed
};

const char* const kUsage =
    "usage: %s [flags]\n"
    "  --bind ADDR              listen address (default 127.0.0.1)\n"
    "  --port N                 listen port (default 6379)\n"
    "  --table custom|std       keyspace hash table (default custom)\n"
    "  --dir PATH               enable persistence in PATH (default: memory only)\n"
    "  --fsync POLICY           always | everysec | no (default everysec; needs --dir)\n"
    "  --snapshot-wal-bytes N   snapshot when the log passes N bytes and the last\n"
    "                           snapshot's size; 0 = only on SAVE/BGSAVE (default 67108864)\n"
    "  --help                   show this text\n"
    "Flags take `--flag value` or `--flag=value`.\n";

template <class T>
bool parse_number(std::string_view s, T* out) {
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(s.data(), end, *out);
  return ec == std::errc() && ptr == end && !s.empty();
}

// returns an error message, empty if all is well
std::string parse_args(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    std::string_view flag = argv[i];
    if (flag == "--help" || flag == "-h") {
      options->help = true;
      return {};
    }
    std::string_view value;
    bool has_value = false;
    if (const size_t eq = flag.find('='); eq != std::string_view::npos) {
      value = flag.substr(eq + 1);
      flag = flag.substr(0, eq);
      has_value = true;
    }
    const bool known = flag == "--port" || flag == "--bind" || flag == "--dir" ||
                       flag == "--fsync" || flag == "--snapshot-wal-bytes" || flag == "--table";
    if (!known) return "unknown flag " + std::string(flag);
    if (!has_value) {
      if (i + 1 == argc) return "missing value for " + std::string(flag);
      value = argv[++i];
    }
    const std::string bad = "invalid value '" + std::string(value) + "' for " + std::string(flag);
    if (flag == "--port") {
      if (!parse_number(value, &options->port)) return bad;
    } else if (flag == "--bind") {
      options->bind = std::string(value);
    } else if (flag == "--dir") {
      options->dir = std::string(value);
    } else if (flag == "--fsync") {
      if (!persist::parse_fsync_policy(value, &options->fsync)) return bad;
      options->persistence_flag_given = true;
    } else if (flag == "--snapshot-wal-bytes") {
      if (!parse_number(value, &options->snapshot_wal_bytes)) return bad;
      options->persistence_flag_given = true;
    } else if (flag == "--table") {
      if (value != "custom" && value != "std") return bad;
      options->table = std::string(value);
    }
  }
  if (options->persistence_flag_given && options->dir.empty()) {
    return "--fsync and --snapshot-wal-bytes need --dir";
  }
  return {};
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  const std::string problem = parse_args(argc, argv, &options);
  if (!problem.empty()) {
    std::fprintf(stderr, "error: %s\n", problem.c_str());
    std::fprintf(stderr, kUsage, argv[0]);
    return 2;
  }
  if (options.help) {
    std::printf(kUsage, argv[0]);
    return 0;
  }
  try {
    signal(SIGPIPE, SIG_IGN);
    net::EventLoop loop;
    SignalWatcher signals(loop);
    std::unique_ptr<engine::Table> table;
    if (options.table == "std") {
      table = std::make_unique<engine::StdTable>();
    } else {
      table = std::make_unique<engine::HashTable>();
    }
    std::unique_ptr<persist::Persistence> persistence;
    if (!options.dir.empty()) {
      persistence = std::make_unique<persist::Persistence>(options.dir, options.fsync,
                                                           options.snapshot_wal_bytes);
    }
    engine::Dispatcher dispatcher(std::move(table), persistence.get());
    if (persistence) persistence->recover(dispatcher.store());
    loop.set_periodic(100, [&dispatcher] { return dispatcher.cron(); });
    net::Server server(loop, dispatcher, options.bind, options.port);
    std::fprintf(stderr, "config: table=%s dir=%s fsync=%s snapshot-wal-bytes=%llu\n",
                 options.table.c_str(), options.dir.empty() ? "(none)" : options.dir.c_str(),
                 options.dir.empty() ? "n/a"
                 : options.fsync == persist::FsyncPolicy::kAlways     ? "always"
                 : options.fsync == persist::FsyncPolicy::kEverySec ? "everysec"
                                                                    : "no",
                 static_cast<unsigned long long>(options.snapshot_wal_bytes));
    std::fprintf(stderr, "listening on %s:%u\n", options.bind.c_str(),
                 static_cast<unsigned>(options.port));
    loop.run();
    std::fprintf(stderr, "shutting down\n");
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
    return 1;
  }
  return 0;
}
