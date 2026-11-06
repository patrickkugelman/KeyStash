#include "engine/store.h"

namespace engine {

const std::string& Store::probe(std::string_view key) {
  probe_.assign(key.data(), key.size());
  return probe_;
}

void Store::set(std::string_view key, std::string_view value) {
  map_[probe(key)].assign(value.data(), value.size());
}

const std::string* Store::get(std::string_view key) {
  auto it = map_.find(probe(key));
  return it == map_.end() ? nullptr : &it->second;
}

bool Store::del(std::string_view key) { return map_.erase(probe(key)) != 0; }

bool Store::exists(std::string_view key) { return map_.find(probe(key)) != map_.end(); }

}  // namespace engine
