#include "net/event_loop.h"

#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <ctime>
#include <cstdlib>
#include <stdexcept>
#include <utility>

namespace net {
namespace {

int64_t monotonic_ms() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

[[noreturn]] void die(const char* what) {
  std::perror(what);
  std::abort();
}

}  // namespace

EventLoop::EventLoop() : epoll_fd_(epoll_create1(EPOLL_CLOEXEC)) {
  if (epoll_fd_ < 0) throw std::runtime_error("epoll_create1 failed");
}

EventLoop::~EventLoop() { ::close(epoll_fd_); }

bool EventLoop::add(int fd, uint32_t events, EventHandler* handler) {
  epoll_event ev{};
  ev.events = events;
  ev.data.ptr = handler;
  return epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == 0;
}

void EventLoop::modify(int fd, uint32_t events, EventHandler* handler) {
  epoll_event ev{};
  ev.events = events;
  ev.data.ptr = handler;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) != 0) die("epoll_ctl(MOD)");
}

void EventLoop::remove(int fd) {
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0) die("epoll_ctl(DEL)");
}

void EventLoop::retire(std::unique_ptr<EventHandler> handler) {
  retired_.push_back(std::move(handler));
}

void EventLoop::set_periodic(int interval_ms, std::function<bool()> fn) {
  periodic_ = std::move(fn);
  interval_ms_ = interval_ms;
  next_tick_ms_ = monotonic_ms() + interval_ms;
}

void EventLoop::run() {
  epoll_event events[256];
  running_ = true;
  while (running_) {
    int timeout = -1;
    if (periodic_) {
      timeout = static_cast<int>(std::max<int64_t>(0, next_tick_ms_ - monotonic_ms()));
    }
    const int n = epoll_wait(epoll_fd_, events, 256, timeout);
    if (n < 0) {
      if (errno == EINTR) continue;
      die("epoll_wait");
    }
    for (int i = 0; i < n; ++i) {
      static_cast<EventHandler*>(events[i].data.ptr)->on_event(events[i].events);
    }
    retired_.clear();
    if (periodic_ && running_) {
      const int64_t now = monotonic_ms();
      if (now >= next_tick_ms_) {
        next_tick_ms_ = periodic_() ? now : now + interval_ms_;
      }
    }
  }
}

}  // namespace net
