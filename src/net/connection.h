#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "net/buffer.h"
#include "net/event_loop.h"
#include "proto/resp.h"

namespace net {

// Runs one parsed command and appends its reply to `out`.
class CommandExecutor {
 public:
  virtual ~CommandExecutor() = default;
  virtual void execute(const std::vector<std::string_view>& args, Buffer& out) = 0;
};

class Server;

class Connection : public EventHandler {
 public:
  Connection(int fd, Server& server, EventLoop& loop, CommandExecutor& executor);
  ~Connection() override;
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  int fd() const { return fd_; }
  void on_event(uint32_t events) override;

 private:
  // Replies above this pause command execution, bounding output memory to the
  // high mark plus one reply. Queued input is capped at proto::kMaxRequest.
  static constexpr size_t kOutHighWater = 1024 * 1024;
  static constexpr size_t kOutLowWater = 256 * 1024;

  bool read_input();
  void process_input();
  bool flush_output();
  void advance();
  void update_interest();
  void close_now();

  int fd_;
  Server& server_;
  EventLoop& loop_;
  CommandExecutor& executor_;
  Buffer in_;
  Buffer out_;
  proto::Parser parser_;
  uint32_t interest_;
  bool peer_done_ = false;           // peer sent FIN; replies may still go out
  bool close_after_flush_ = false;   // protocol error reply is queued
  bool paused_ = false;              // reply backlog above the high mark
  bool closed_ = false;
};

}  // namespace net
