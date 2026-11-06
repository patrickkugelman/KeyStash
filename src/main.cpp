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

namespace {

// Turns SIGINT/SIGTERM into a clean loop exit.
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
};

bool parse_args(int argc, char** argv, Options* options) {
  for (int i = 1; i < argc; ++i) {
    const std::string_view flag = argv[i];
    if (i + 1 == argc) return false;
    const std::string_view value = argv[++i];
    if (flag == "--port") {
      const char* end = value.data() + value.size();
      auto [ptr, ec] = std::from_chars(value.data(), end, options->port);
      if (ec != std::errc() || ptr != end) return false;
    } else if (flag == "--bind") {
      options->bind = std::string(value);
    } else if (flag == "--table") {
      if (value != "custom" && value != "std") return false;
      options->table = std::string(value);
    } else {
      return false;
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_args(argc, argv, &options)) {
    std::fprintf(stderr, "usage: %s [--bind ADDR] [--port N] [--table custom|std]\n", argv[0]);
    return 2;
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
    engine::Dispatcher dispatcher(std::move(table));
    loop.set_periodic(100, [&dispatcher] { return dispatcher.cron(); });
    net::Server server(loop, dispatcher, options.bind, options.port);
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
