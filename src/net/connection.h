#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "net/buffer.h"
#include "net/event_loop.h"
#include "proto/resp.h"

namespace net {

// runs one parsed command, the reply goes to out
class CommandExecutor {
 public:
  virtual ~CommandExecutor() = default;
  virtual void execute(const std::vector<std::string_view>& args, Buffer& out) = 0;
  // once per loop batch, after the commands ran and before any reply is sent
  virtual void commit() {}
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

  // server calls process() on every queued connection, then commit() once,
  // then finish() on all of them
  void process();
  void finish();

  bool queued() const { return queued_; }
  void set_queued(bool queued) { queued_ = queued; }

 private:
  // above the high mark we stop running commands until the client reads
  static constexpr size_t kOutHighWater = 1024 * 1024;
  static constexpr size_t kOutLowWater = 256 * 1024;

  bool read_input();
  void process_input();
  bool flush_output();
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
  bool peer_done_ = false;           // got FIN, replies can still go out
  bool close_after_flush_ = false;   // protocol error queued, close after it
  bool paused_ = false;              // too much unsent reply
  bool closed_ = false;
  bool queued_ = false;              // already in the server's list
};

}  // namespace net
