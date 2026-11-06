#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "engine/table.h"

namespace engine {

// Keyspace with TTLs. Expired keys are removed lazily on access and by an
// active sweep driven from cron(). Expiry times are absolute unix ms.
class Store {
 public:
  explicit Store(std::unique_ptr<Table> table);

  int64_t now_ms() const;
  size_t size() const { return table_->size(); }

  // Overwrites the value and clears any TTL.
  void set(std::string_view key, std::string_view value);
  // Overwrites the value and keeps any TTL (what INCR does).
  void set_keep_ttl(std::string_view key, std::string_view value);
  // Restores an entry from a snapshot; entries already expired are skipped.
  void load_entry(std::string_view key, std::string_view value, int64_t expire_at_ms);
  // Visits live (unexpired) entries. `fn` must not mutate the store.
  void for_each_live(const std::function<void(std::string_view, const Entry&)>& fn);
  // Null if absent or expired. Valid until the next mutation.
  const std::string* get(std::string_view key);
  bool del(std::string_view key);
  bool exists(std::string_view key);

  enum class ExpireResult { kNoKey, kSet, kDeleted };
  // Deadline at or before now deletes the key.
  ExpireResult expire_at(std::string_view key, int64_t when_ms);
  // -2 no key, -1 no TTL, else remaining ms.
  int64_t ttl_ms(std::string_view key);

  enum class IncrStatus { kOk, kNotInteger, kOverflow };
  // Missing keys count as 0. An existing TTL is kept.
  IncrStatus incr_by(std::string_view key, int64_t delta, int64_t* result);

  // Bounded background work. True means more is pending: call again soon.
  bool cron();

 private:
  struct HeapItem {
    int64_t when;
    std::string key;
  };

  static constexpr size_t kSweepBudget = 5000;
  static constexpr size_t kRehashBuckets = 100;

  Entry* lookup(std::string_view key);
  void erase_entry(std::string_view key, Entry* entry);
  void clear_ttl(Entry& entry);
  void set_deadline(std::string_view key, Entry& entry, int64_t when_ms);
  void rebuild_heap();

  std::unique_ptr<Table> table_;
  // Min-heap on `when`. Entries go stale when a TTL changes or the key
  // dies; a stale entry is detected on pop by comparing with the live TTL.
  std::vector<HeapItem> heap_;
  size_t ttl_count_ = 0;  // keys that currently have a TTL
};

}  // namespace engine
