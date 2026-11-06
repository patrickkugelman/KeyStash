#include "net/connection.h"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <string>

#include "net/server.h"

namespace net {

namespace {
constexpr uint32_t kIn = EPOLLIN;
constexpr uint32_t kOut = EPOLLOUT;
}  // namespace

Connection::Connection(int fd, Server& server, EventLoop& loop, CommandExecutor& executor)
    : fd_(fd), server_(server), loop_(loop), executor_(executor), interest_(kIn) {}

Connection::~Connection() { ::close(fd_); }

void Connection::on_event(uint32_t events) {
  if (closed_) return;
  if ((events & (EPOLLERR | EPOLLHUP)) != 0) {
    close_now();
    return;
  }
  if ((events & EPOLLIN) != 0 && !read_input()) {
    close_now();
    return;
  }
  server_.defer(this);
}

// One read per wakeup: level triggering re-reports any remainder, so a busy
// client cannot starve the others.
bool Connection::read_input() {
  const auto scratch = server_.read_scratch();
  const ssize_t n = ::read(fd_, scratch.first, scratch.second);
  if (n > 0) {
    in_.append(scratch.first, static_cast<size_t>(n));
    return true;
  }
  if (n == 0) {
    peer_done_ = true;
    return true;
  }
  return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
}

void Connection::process_input() {
  while (out_.size() < kOutHighWater) {
    const auto status = parser_.parse(in_.data(), in_.size());
    if (status == proto::Parser::Status::kNeedMore) return;
    if (status == proto::Parser::Status::kError) {
      proto::write_error(out_, "ERR Protocol error: " + parser_.error());
      close_after_flush_ = true;
      in_.clear();
      return;
    }
    if (!parser_.args().empty()) executor_.execute(parser_.args(), out_);
    in_.consume(parser_.consumed());
    parser_.reset();
  }
  paused_ = true;
}

// One send per wakeup; a short write stays queued until EPOLLOUT fires.
bool Connection::flush_output() {
  if (out_.empty()) return true;
  const ssize_t n = ::send(fd_, out_.data(), out_.size(), MSG_NOSIGNAL);
  if (n >= 0) {
    out_.consume(static_cast<size_t>(n));
    return true;
  }
  return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
}

void Connection::process() {
  if (closed_) return;
  if (paused_ && out_.size() <= kOutLowWater) paused_ = false;
  if (!paused_ && !close_after_flush_) process_input();
}

void Connection::finish() {
  if (closed_) return;
  while (true) {
    if (!flush_output()) {
      close_now();
      return;
    }
    if (!paused_ || out_.size() > kOutLowWater) break;
    // Drained enough to resume. Stay here while buffered requests can run:
    // no new EPOLLIN will announce them. Their effects must be durable
    // before the next flush sends the replies.
    paused_ = false;
    if (!close_after_flush_) process_input();
    executor_.commit();
  }
  if (out_.empty() && (close_after_flush_ || peer_done_)) {
    close_now();
    return;
  }
  update_interest();
}

void Connection::update_interest() {
  uint32_t wanted = 0;
  // After FIN the socket stays readable forever, so level triggering would spin.
  // Keep reading while paused: a client that writes its whole pipeline before
  // reading any reply would otherwise deadlock against us.
  if (!peer_done_ && !close_after_flush_ && in_.size() < proto::kMaxRequest) wanted |= kIn;
  if (!out_.empty()) wanted |= kOut;
  if (wanted != interest_) {
    loop_.modify(fd_, wanted, this);
    interest_ = wanted;
  }
}

void Connection::close_now() {
  closed_ = true;
  loop_.remove(fd_);
  server_.release(this);
}

}  // namespace net
