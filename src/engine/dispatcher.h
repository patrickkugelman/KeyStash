#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "engine/store.h"
#include "engine/table.h"
#include "net/connection.h"
#include "persist/persistence.h"

namespace engine {

class Dispatcher : public net::CommandExecutor {
 public:
  // persistence can be null (memory only), recover it into store() first
  explicit Dispatcher(std::unique_ptr<Table> table, persist::Persistence* persistence = nullptr)
      : store_(std::move(table)), persist_(persistence) {}

  Store& store() { return store_; }

  void execute(const std::vector<std::string_view>& args, net::Buffer& out) override;
  void commit() override;

  // expiry sweep, rehash, log sync, snapshots. true = more to do
  bool cron();

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
  void save(const Args& args, net::Buffer& out);
  void bgsave(const Args& args, net::Buffer& out);
  void info(const Args& args, net::Buffer& out);

  void expire_generic(const Args& args, net::Buffer& out, int64_t unit_ms);

  struct Command {
    std::string_view name;
    int arity;  // counts the name, negative = at least -arity
    void (Dispatcher::*handler)(const Args&, net::Buffer&);
  };
  static const Command kCommands[];

  Store store_;
  persist::Persistence* persist_;
};

}  // namespace engine
