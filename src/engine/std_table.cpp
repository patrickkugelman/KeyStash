#include "engine/std_table.h"

namespace engine {

const std::string& StdTable::probe(std::string_view key) {
  probe_.assign(key.data(), key.size());
  return probe_;
}

Entry* StdTable::find(std::string_view key) {
  auto it = map_.find(probe(key));
  return it == map_.end() ? nullptr : &it->second;
}

Entry* StdTable::find_or_insert(std::string_view key, bool* inserted) {
  auto result = map_.try_emplace(probe(key));
  *inserted = result.second;
  return &result.first->second;
}

bool StdTable::erase(std::string_view key) { return map_.erase(probe(key)) != 0; }

void StdTable::for_each(const std::function<void(std::string_view, const Entry&)>& fn) const {
  for (const auto& kv : map_) fn(kv.first, kv.second);
}

}  // namespace engine
