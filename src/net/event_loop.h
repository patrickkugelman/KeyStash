#pragma once

#include <cstdint>
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

  void run();
  void stop() { running_ = false; }

 private:
  int epoll_fd_;
  bool running_ = false;
  std::vector<std::unique_ptr<EventHandler>> retired_;
};

}  // namespace net
