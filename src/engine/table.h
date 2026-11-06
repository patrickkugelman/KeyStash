#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace engine {

struct Entry {
  std::string value;
  int64_t expire_at_ms = -1;  // absolute unix ms; -1 means no TTL
};

// Key -> Entry map. Single-threaded. Returned Entry pointers stay valid until
// that key is erased, even across inserts and rehashing.
class Table {
 public:
  virtual ~Table() = default;

  virtual Entry* find(std::string_view key) = 0;
  // Returns the entry for `key`, creating an empty one if absent.
  virtual Entry* find_or_insert(std::string_view key, bool* inserted) = 0;
  virtual bool erase(std::string_view key) = 0;
  virtual size_t size() const = 0;
  // Visits every entry once, in no particular order. `fn` must not mutate the table.
  virtual void for_each(const std::function<void(std::string_view, const Entry&)>& fn) const = 0;

  // Idle-time maintenance: moves up to `buckets` buckets of a pending rehash.
  // Returns true while more work remains.
  virtual bool rehash_some(size_t buckets) {
    (void)buckets;
    return false;
  }
};

}  // namespace engine
