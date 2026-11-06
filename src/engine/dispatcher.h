#pragma once

#include <string_view>
#include <vector>

#include "engine/store.h"
#include "net/connection.h"

namespace engine {

class Dispatcher : public net::CommandExecutor {
 public:
  void execute(const std::vector<std::string_view>& args, net::Buffer& out) override;

 private:
  using Args = std::vector<std::string_view>;

  void ping(const Args& args, net::Buffer& out);
  void set(const Args& args, net::Buffer& out);
  void get(const Args& args, net::Buffer& out);
  void del(const Args& args, net::Buffer& out);
  void exists(const Args& args, net::Buffer& out);

  struct Command {
    std::string_view name;
    int arity;  // includes the name; negative means "at least -arity"
    void (Dispatcher::*handler)(const Args&, net::Buffer&);
  };
  static const Command kCommands[];

  Store store_;
};

}  // namespace engine
