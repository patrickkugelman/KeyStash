#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace net {

class EventHandler {
 public:
  virtual ~EventHandler() = default;
  virtual void on_event(uint32_t events) = 0;
};

// Level-triggered epoll loop.
class EventLoop {
 public:
  EventLoop();
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // False if the kernel refused the fd (e.g. watch limit reached).
  bool add(int fd, uint32_t events, EventHandler* handler);
  void modify(int fd, uint32_t events, EventHandler* handler);
  void remove(int fd);

  // Destroys the handler after the current batch, so events still queued in
  // that batch can never reach a freed handler or a reused fd.
  void retire(std::unique_ptr<EventHandler> handler);

  // Calls `fn` every `interval_ms` from the loop thread. If it returns true
  // it is called again as soon as pending events have been handled.
  void set_periodic(int interval_ms, std::function<bool()> fn);

  void run();
  void stop() { running_ = false; }

 private:
  int epoll_fd_;
  bool running_ = false;
  std::function<bool()> periodic_;
  int64_t interval_ms_ = 0;
  int64_t next_tick_ms_ = 0;
  std::vector<std::unique_ptr<EventHandler>> retired_;
};

}  // namespace net
