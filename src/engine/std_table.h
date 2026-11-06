#pragma once

#include <string>
#include <unordered_map>

#include "engine/table.h"

namespace engine {

// plain std::unordered_map version
class StdTable : public Table {
 public:
  Entry* find(std::string_view key) override;
  Entry* find_or_insert(std::string_view key, bool* inserted) override;
  bool erase(std::string_view key) override;
  size_t size() const override { return map_.size(); }
  void for_each(const std::function<void(std::string_view, const Entry&)>& fn) const override;

 private:
  // no heterogeneous lookup in c++17, so reuse one string for the probe
  const std::string& probe(std::string_view key);

  std::unordered_map<std::string, Entry> map_;
  std::string probe_;
};

}  // namespace engine
