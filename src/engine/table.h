#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace engine {

struct Entry {
  std::string value;
  int64_t expire_at_ms = -1;  // unix ms, -1 = no ttl
};

// key -> Entry, single threaded. an Entry pointer stays valid until its key
// is erased, rehashing included
class Table {
 public:
  virtual ~Table() = default;

  virtual Entry* find(std::string_view key) = 0;
  virtual Entry* find_or_insert(std::string_view key, bool* inserted) = 0;
  virtual bool erase(std::string_view key) = 0;
  virtual size_t size() const = 0;
  // every entry once, any order, fn must not modify the table
  virtual void for_each(const std::function<void(std::string_view, const Entry&)>& fn) const = 0;

  // moves up to n buckets of a pending rehash, true while there is more
  virtual bool rehash_some(size_t buckets) {
    (void)buckets;
    return false;
  }
};

}  // namespace engine
