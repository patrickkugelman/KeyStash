#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "net/connection.h"
#include "net/event_loop.h"

namespace net {

// listening socket and all connections
class Server : public EventHandler {
 public:
  Server(EventLoop& loop, CommandExecutor& executor, const std::string& bind_addr, uint16_t port);
  ~Server() override;
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  void on_event(uint32_t events) override;

  void release(Connection* connection);

  // run this connection in the end-of-batch pass
  void defer(Connection* connection);

  // read buffer shared by all connections
  std::pair<char*, size_t> read_scratch() { return {read_scratch_.get(), kReadChunk}; }

 private:
  static constexpr size_t kReadChunk = 64 * 1024;

  void shed_one_client();
  void end_of_batch();

  EventLoop& loop_;
  CommandExecutor& executor_;
  int listen_fd_ = -1;
  int spare_fd_ = -1;  // for rejecting clients at the fd limit
  std::unique_ptr<char[]> read_scratch_;
  std::unordered_map<Connection*, std::unique_ptr<Connection>> connections_;
  std::vector<Connection*> pending_;  // work queued in this batch
};

}  // namespace net
