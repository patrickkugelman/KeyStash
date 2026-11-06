#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

namespace engine {

class Store {
 public:
  void set(std::string_view key, std::string_view value);
  // Null if absent. Valid until the next mutation.
  const std::string* get(std::string_view key);
  bool del(std::string_view key);
  bool exists(std::string_view key);

 private:
  // C++17 maps have no heterogeneous lookup; reuse one string for probes.
  const std::string& probe(std::string_view key);

  std::unordered_map<std::string, std::string> map_;
  std::string probe_;
};

}  // namespace engine
