#include "net/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace net {
namespace {

constexpr int kBacklog = 511;
// Bounds accept work per wakeup so a connection storm cannot starve clients.
constexpr int kMaxAcceptsPerEvent = 256;

std::runtime_error sys_error(const std::string& what) {
  return std::runtime_error(what + ": " + std::strerror(errno));
}

int open_spare_fd() { return ::open("/dev/null", O_RDONLY | O_CLOEXEC); }

}  // namespace

Server::Server(EventLoop& loop, CommandExecutor& executor, const std::string& bind_addr,
               uint16_t port)
    : loop_(loop), executor_(executor), read_scratch_(new char[kReadChunk]) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, bind_addr.c_str(), &addr.sin_addr) != 1) {
    throw std::runtime_error("invalid bind address: " + bind_addr);
  }
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listen_fd_ < 0) throw sys_error("socket");
  try {
    const int on = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
      throw sys_error("setsockopt");
    }
    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
      throw sys_error("bind " + bind_addr + ":" + std::to_string(port));
    }
    if (::listen(listen_fd_, kBacklog) != 0) throw sys_error("listen");
    spare_fd_ = open_spare_fd();
    if (spare_fd_ < 0) throw sys_error("open /dev/null");
    if (!loop_.add(listen_fd_, EPOLLIN, this)) throw sys_error("epoll add");
  } catch (...) {
    ::close(listen_fd_);
    if (spare_fd_ >= 0) ::close(spare_fd_);
    throw;
  }
}

Server::~Server() {
  ::close(listen_fd_);
  ::close(spare_fd_);
}

void Server::on_event(uint32_t) {
  for (int i = 0; i < kMaxAcceptsPerEvent; ++i) {
    const int fd = accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EMFILE || errno == ENFILE) {
        shed_one_client();
        continue;
      }
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if (errno != EAGAIN && errno != EWOULDBLOCK) std::perror("accept4");
      return;
    }
    const int on = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    auto connection = std::make_unique<Connection>(fd, *this, loop_, executor_);
    Connection* raw = connection.get();
    if (!loop_.add(fd, EPOLLIN, raw)) {
      std::perror("epoll add client");
      continue;  // destroying the connection closes fd
    }
    connections_.emplace(raw, std::move(connection));
  }
}

// Level triggering would spin on a pending connection we cannot accept, so
// free a descriptor, take that client and hang up on it.
void Server::shed_one_client() {
  ::close(spare_fd_);
  const int fd = accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
  if (fd >= 0) ::close(fd);
  spare_fd_ = open_spare_fd();
}

void Server::release(Connection* connection) {
  auto it = connections_.find(connection);
  if (it == connections_.end()) return;
  loop_.retire(std::move(it->second));
  connections_.erase(it);
}

}  // namespace net
