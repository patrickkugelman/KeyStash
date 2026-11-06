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

// keyspace with ttls. expired keys go lazily on access and from the sweep
// in cron(). times are absolute unix ms
class Store {
 public:
  explicit Store(std::unique_ptr<Table> table);

  int64_t now_ms() const;
  size_t size() const { return table_->size(); }

  // overwrite the value, clear the ttl
  void set(std::string_view key, std::string_view value);
  // overwrite the value, keep the ttl (INCR)
  void set_keep_ttl(std::string_view key, std::string_view value);
  // from a snapshot, already expired entries are skipped
  void load_entry(std::string_view key, std::string_view value, int64_t expire_at_ms);
  // live entries only, fn must not modify the store
  void for_each_live(const std::function<void(std::string_view, const Entry&)>& fn);
  // null if missing or expired, valid until the next change
  const std::string* get(std::string_view key);
  bool del(std::string_view key);
  bool exists(std::string_view key);

  enum class ExpireResult { kNoKey, kSet, kDeleted };
  // a time in the past deletes the key
  ExpireResult expire_at(std::string_view key, int64_t when_ms);
  // -2 no key, -1 no ttl, else ms left
  int64_t ttl_ms(std::string_view key);

  enum class IncrStatus { kOk, kNotInteger, kOverflow };
  // missing counts as 0, the ttl is kept
  IncrStatus incr_by(std::string_view key, int64_t delta, int64_t* result);

  // bounded work per call, true = call again soon
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
  // min-heap on when. items go stale when a ttl changes or the key dies,
  // checked on pop against the live ttl
  std::vector<HeapItem> heap_;
  size_t ttl_count_ = 0;  // keys that have a ttl
};

}  // namespace engine
