#include "engine/dispatcher.h"

#include <algorithm>
#include <cstdint>
#include <string>

#include "engine/numeric.h"
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

std::string lowercase(std::string_view s) {
  std::string r(s);
  for (char& c : r) c = lower(c);
  return r;
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
    {"ping", -1, &Dispatcher::ping},     {"echo", 2, &Dispatcher::echo},
    {"set", -3, &Dispatcher::set},       {"get", 2, &Dispatcher::get},
    {"del", -2, &Dispatcher::del},       {"exists", -2, &Dispatcher::exists},
    {"expire", 3, &Dispatcher::expire},  {"pexpire", 3, &Dispatcher::pexpire},
    {"ttl", 2, &Dispatcher::ttl},        {"pttl", 2, &Dispatcher::pttl},
    {"incr", 2, &Dispatcher::incr},      {"dbsize", 1, &Dispatcher::dbsize},
    {"save", 1, &Dispatcher::save},
};

void Dispatcher::commit() {
  if (persist_ != nullptr) persist_->commit();
}

bool Dispatcher::cron() {
  const bool more = store_.cron();
  if (persist_ != nullptr) persist_->cron(store_);
  return more;
}

void Dispatcher::save(const Args&, net::Buffer& out) {
  if (persist_ == nullptr) {
    proto::write_error(out, "ERR persistence is not enabled");
    return;
  }
  std::string error;
  if (persist_->snapshot(store_, &error)) {
    proto::write_simple(out, "OK");
  } else {
    proto::write_error(out, "ERR snapshot failed: " + error);
  }
}

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
  if (persist_ != nullptr) persist_->log_set(args[1], args[2]);
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
  for (size_t i = 1; i < args.size(); ++i) {
    if (!store_.del(args[i])) continue;
    ++removed;
    if (persist_ != nullptr) persist_->log_del(args[i]);
  }
  proto::write_integer(out, removed);
}

void Dispatcher::exists(const Args& args, net::Buffer& out) {
  int64_t found = 0;
  for (size_t i = 1; i < args.size(); ++i) found += store_.exists(args[i]) ? 1 : 0;
  proto::write_integer(out, found);
}

void Dispatcher::expire(const Args& args, net::Buffer& out) { expire_generic(args, out, 1000); }

void Dispatcher::pexpire(const Args& args, net::Buffer& out) { expire_generic(args, out, 1); }

void Dispatcher::expire_generic(const Args& args, net::Buffer& out, int64_t unit_ms) {
  int64_t amount = 0;
  if (!parse_int64(args[2], &amount)) {
    proto::write_error(out, "ERR value is not an integer or out of range");
    return;
  }
  int64_t delta = 0;
  int64_t when = 0;
  if (__builtin_mul_overflow(amount, unit_ms, &delta) ||
      __builtin_add_overflow(delta, store_.now_ms(), &when)) {
    proto::write_error(out, "ERR invalid expire time in '" + lowercase(args[0]) + "' command");
    return;
  }
  // Logged as an absolute time so replay does not depend on when it runs.
  const Store::ExpireResult result = store_.expire_at(args[1], when);
  if (persist_ != nullptr) {
    if (result == Store::ExpireResult::kSet) persist_->log_expire_at(args[1], when);
    if (result == Store::ExpireResult::kDeleted) persist_->log_del(args[1]);
  }
  proto::write_integer(out, result == Store::ExpireResult::kNoKey ? 0 : 1);
}

void Dispatcher::ttl(const Args& args, net::Buffer& out) {
  const int64_t ms = store_.ttl_ms(args[1]);
  proto::write_integer(out, ms < 0 ? ms : (ms + 500) / 1000);
}

void Dispatcher::pttl(const Args& args, net::Buffer& out) {
  proto::write_integer(out, store_.ttl_ms(args[1]));
}

void Dispatcher::incr(const Args& args, net::Buffer& out) {
  int64_t result = 0;
  switch (store_.incr_by(args[1], 1, &result)) {
    case Store::IncrStatus::kOk:
      // Logged as the resulting value so replay never depends on prior state.
      if (persist_ != nullptr) persist_->log_set_keep_ttl(args[1], std::to_string(result));
      proto::write_integer(out, result);
      break;
    case Store::IncrStatus::kNotInteger:
      proto::write_error(out, "ERR value is not an integer or out of range");
      break;
    case Store::IncrStatus::kOverflow:
      proto::write_error(out, "ERR increment or decrement would overflow");
      break;
  }
}

void Dispatcher::dbsize(const Args&, net::Buffer& out) {
  proto::write_integer(out, static_cast<int64_t>(store_.size()));
}

}  // namespace engine
