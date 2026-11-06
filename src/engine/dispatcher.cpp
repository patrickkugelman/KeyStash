#include "engine/dispatcher.h"

#include <algorithm>
#include <cstdint>
#include <string>

#include "proto/resp.h"

namespace engine {
namespace {

constexpr size_t kMaxEchoedBytes = 128;

char lower(char c) { return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : c; }

bool equals_ignore_case(std::string_view a, std::string_view lowercase) {
  return a.size() == lowercase.size() &&
         std::equal(a.begin(), a.end(), lowercase.begin(),
                    [](char x, char y) { return lower(x) == y; });
}

void write_unknown_command(const std::vector<std::string_view>& args, net::Buffer& out) {
  std::string message = "ERR unknown command '";
  message.append(args[0].substr(0, kMaxEchoedBytes));
  message.append("', with args beginning with: ");
  size_t budget = kMaxEchoedBytes;
  for (size_t i = 1; i < args.size() && budget > 0; ++i) {
    const std::string_view shown = args[i].substr(0, budget);
    message.append("'").append(shown).append("' ");
    budget -= shown.size();
  }
  proto::write_error(out, message);
}

}  // namespace

const Dispatcher::Command Dispatcher::kCommands[] = {
    {"ping", -1, &Dispatcher::ping},   {"set", -3, &Dispatcher::set},
    {"get", 2, &Dispatcher::get},      {"del", -2, &Dispatcher::del},
    {"exists", -2, &Dispatcher::exists}, {"echo", 2, &Dispatcher::echo},
};

void Dispatcher::execute(const Args& args, net::Buffer& out) {
  for (const Command& command : kCommands) {
    if (!equals_ignore_case(args[0], command.name)) continue;
    const auto argc = static_cast<int64_t>(args.size());
    if (command.arity >= 0 ? argc != command.arity : argc < -command.arity) {
      proto::write_error(out, "ERR wrong number of arguments for '" + std::string(command.name) +
                                  "' command");
      return;
    }
    (this->*command.handler)(args, out);
    return;
  }
  write_unknown_command(args, out);
}

void Dispatcher::ping(const Args& args, net::Buffer& out) {
  if (args.size() > 2) {
    proto::write_error(out, "ERR wrong number of arguments for 'ping' command");
  } else if (args.size() == 2) {
    proto::write_bulk(out, args[1]);
  } else {
    proto::write_simple(out, "PONG");
  }
}

// redis-cli --pipe ends its stream with ECHO <marker> and waits for the reply.
void Dispatcher::echo(const Args& args, net::Buffer& out) { proto::write_bulk(out, args[1]); }

void Dispatcher::set(const Args& args, net::Buffer& out) {
  if (args.size() != 3) {
    proto::write_error(out, "ERR syntax error");
    return;
  }
  store_.set(args[1], args[2]);
  proto::write_simple(out, "OK");
}

void Dispatcher::get(const Args& args, net::Buffer& out) {
  const std::string* value = store_.get(args[1]);
  if (value == nullptr) {
    proto::write_null(out);
  } else {
    proto::write_bulk(out, *value);
  }
}

void Dispatcher::del(const Args& args, net::Buffer& out) {
  int64_t removed = 0;
  for (size_t i = 1; i < args.size(); ++i) removed += store_.del(args[i]) ? 1 : 0;
  proto::write_integer(out, removed);
}

void Dispatcher::exists(const Args& args, net::Buffer& out) {
  int64_t found = 0;
  for (size_t i = 1; i < args.size(); ++i) found += store_.exists(args[i]) ? 1 : 0;
  proto::write_integer(out, found);
}

}  // namespace engine
