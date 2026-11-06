#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "net/connection.h"
#include "net/event_loop.h"

namespace net {

// Owns the listening socket and all connections.
class Server : public EventHandler {
 public:
  // Throws std::runtime_error if it cannot bind.
  Server(EventLoop& loop, CommandExecutor& executor, const std::string& bind_addr, uint16_t port);
  ~Server() override;
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  void on_event(uint32_t events) override;

  // Called by a connection that has left the event loop.
  void release(Connection* connection);

  // Shared by all connections; valid only during one on_event call.
  std::pair<char*, size_t> read_scratch() { return {read_scratch_.get(), kReadChunk}; }

 private:
  static constexpr size_t kReadChunk = 64 * 1024;

  void shed_one_client();

  EventLoop& loop_;
  CommandExecutor& executor_;
  int listen_fd_ = -1;
  int spare_fd_ = -1;  // lets us reject clients when out of descriptors
  std::unique_ptr<char[]> read_scratch_;
  std::unordered_map<Connection*, std::unique_ptr<Connection>> connections_;
};

}  // namespace net
