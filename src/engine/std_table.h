#pragma once

#include <string>
#include <unordered_map>

#include "engine/table.h"

namespace engine {

// Reference implementation on std::unordered_map.
class StdTable : public Table {
 public:
  Entry* find(std::string_view key) override;
  Entry* find_or_insert(std::string_view key, bool* inserted) override;
  bool erase(std::string_view key) override;
  size_t size() const override { return map_.size(); }
  void for_each(const std::function<void(std::string_view, const Entry&)>& fn) const override;

 private:
  // C++17 maps have no heterogeneous lookup; reuse one string for probes.
  const std::string& probe(std::string_view key);

  std::unordered_map<std::string, Entry> map_;
  std::string probe_;
};

}  // namespace engine
