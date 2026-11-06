#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "engine/store.h"
#include "engine/table.h"
#include "net/connection.h"

namespace engine {

class Dispatcher : public net::CommandExecutor {
 public:
  explicit Dispatcher(std::unique_ptr<Table> table) : store_(std::move(table)) {}

  void execute(const std::vector<std::string_view>& args, net::Buffer& out) override;

  // Periodic maintenance (active expiry, rehash). True if more work is pending.
  bool cron() { return store_.cron(); }

 private:
  using Args = std::vector<std::string_view>;

  void ping(const Args& args, net::Buffer& out);
  void echo(const Args& args, net::Buffer& out);
  void set(const Args& args, net::Buffer& out);
  void get(const Args& args, net::Buffer& out);
  void del(const Args& args, net::Buffer& out);
  void exists(const Args& args, net::Buffer& out);
  void expire(const Args& args, net::Buffer& out);
  void pexpire(const Args& args, net::Buffer& out);
  void ttl(const Args& args, net::Buffer& out);
  void pttl(const Args& args, net::Buffer& out);
  void incr(const Args& args, net::Buffer& out);
  void dbsize(const Args& args, net::Buffer& out);

  void expire_generic(const Args& args, net::Buffer& out, int64_t unit_ms);

  struct Command {
    std::string_view name;
    int arity;  // includes the name; negative means "at least -arity"
    void (Dispatcher::*handler)(const Args&, net::Buffer&);
  };
  static const Command kCommands[];

  Store store_;
};

}  // namespace engine
