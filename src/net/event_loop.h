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

// epoll loop, level triggered
class EventLoop {
 public:
  EventLoop();
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  bool add(int fd, uint32_t events, EventHandler* handler);
  void modify(int fd, uint32_t events, EventHandler* handler);
  void remove(int fd);

  // deleted after this batch, queued events may still point at it
  void retire(std::unique_ptr<EventHandler> handler);

  // fn returning true means more work, it runs again after the next events
  void set_periodic(int interval_ms, std::function<bool()> fn);

  // runs after each batch of events, before sleeping again
  void set_after_events(std::function<void()> fn) { after_events_ = std::move(fn); }

  void run();
  void stop() { running_ = false; }

 private:
  int epoll_fd_;
  bool running_ = false;
  std::function<void()> after_events_;
  std::function<bool()> periodic_;
  int64_t interval_ms_ = 0;
  int64_t next_tick_ms_ = 0;
  std::vector<std::unique_ptr<EventHandler>> retired_;
};

}  // namespace net
