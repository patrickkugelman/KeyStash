#include "engine/store.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include "engine/numeric.h"

namespace engine {
namespace {

template <class T>
bool later(const T& a, const T& b) {
  return a.when > b.when;
}

}  // namespace

Store::Store(std::unique_ptr<Table> table) : table_(std::move(table)) {}

int64_t Store::now_ms() const {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Entry* Store::lookup(std::string_view key) {
  Entry* e = table_->find(key);
  if (e != nullptr && e->expire_at_ms >= 0 && now_ms() > e->expire_at_ms) {
    erase_entry(key, e);
    return nullptr;
  }
  return e;
}

void Store::erase_entry(std::string_view key, Entry* entry) {
  if (entry->expire_at_ms >= 0) --ttl_count_;
  table_->erase(key);
}

void Store::clear_ttl(Entry& entry) {
  if (entry.expire_at_ms >= 0) {
    --ttl_count_;
    entry.expire_at_ms = -1;
  }
}

void Store::set(std::string_view key, std::string_view value) {
  bool inserted = false;
  Entry* e = table_->find_or_insert(key, &inserted);
  e->value.assign(value.data(), value.size());
  clear_ttl(*e);
}

void Store::set_keep_ttl(std::string_view key, std::string_view value) {
  bool inserted = false;
  table_->find_or_insert(key, &inserted)->value.assign(value.data(), value.size());
}

void Store::load_entry(std::string_view key, std::string_view value, int64_t expire_at_ms) {
  if (expire_at_ms >= 0 && now_ms() > expire_at_ms) return;
  set(key, value);
  if (expire_at_ms >= 0) {
    bool inserted = false;
    set_deadline(key, *table_->find_or_insert(key, &inserted), expire_at_ms);
  }
}

void Store::for_each_live(const std::function<void(std::string_view, const Entry&)>& fn) {
  const int64_t now = now_ms();
  table_->for_each([&](std::string_view key, const Entry& entry) {
    if (entry.expire_at_ms >= 0 && now > entry.expire_at_ms) return;
    fn(key, entry);
  });
}

const std::string* Store::get(std::string_view key) {
  Entry* e = lookup(key);
  return e == nullptr ? nullptr : &e->value;
}

bool Store::del(std::string_view key) {
  Entry* e = lookup(key);
  if (e == nullptr) return false;
  erase_entry(key, e);
  return true;
}

bool Store::exists(std::string_view key) { return lookup(key) != nullptr; }

Store::ExpireResult Store::expire_at(std::string_view key, int64_t when_ms) {
  Entry* e = lookup(key);
  if (e == nullptr) return ExpireResult::kNoKey;
  if (when_ms <= now_ms()) {
    erase_entry(key, e);
    return ExpireResult::kDeleted;
  }
  set_deadline(key, *e, when_ms);
  return ExpireResult::kSet;
}

void Store::set_deadline(std::string_view key, Entry& entry, int64_t when_ms) {
  if (entry.expire_at_ms == when_ms) return;  // already in the heap
  if (entry.expire_at_ms < 0) ++ttl_count_;
  entry.expire_at_ms = when_ms;
  heap_.push_back({when_ms, std::string(key)});
  std::push_heap(heap_.begin(), heap_.end(), later<HeapItem>);
  // EXPIRE on the same key over and over leaves stale items, cap them
  if (heap_.size() > 2 * ttl_count_ + 1024) rebuild_heap();
}

// drop stale and duplicate items, one per key with a ttl is left
void Store::rebuild_heap() {
  std::vector<HeapItem> live;
  live.reserve(ttl_count_);
  for (HeapItem& item : heap_) {
    const Entry* e = table_->find(item.key);
    if (e != nullptr && e->expire_at_ms == item.when) live.push_back(std::move(item));
  }
  std::sort(live.begin(), live.end(), [](const HeapItem& a, const HeapItem& b) {
    return a.when != b.when ? a.when < b.when : a.key < b.key;
  });
  live.erase(std::unique(live.begin(), live.end(),
                         [](const HeapItem& a, const HeapItem& b) {
                           return a.when == b.when && a.key == b.key;
                         }),
             live.end());
  heap_ = std::move(live);
  std::make_heap(heap_.begin(), heap_.end(), later<HeapItem>);
}

int64_t Store::ttl_ms(std::string_view key) {
  const Entry* e = lookup(key);
  if (e == nullptr) return -2;
  if (e->expire_at_ms < 0) return -1;
  return std::max<int64_t>(0, e->expire_at_ms - now_ms());
}

Store::IncrStatus Store::incr_by(std::string_view key, int64_t delta, int64_t* result) {
  Entry* e = lookup(key);
  int64_t current = 0;
  if (e != nullptr && !parse_int64(e->value, &current)) return IncrStatus::kNotInteger;
  int64_t next = 0;
  if (__builtin_add_overflow(current, delta, &next)) return IncrStatus::kOverflow;
  if (e == nullptr) {
    bool inserted = false;
    e = table_->find_or_insert(key, &inserted);
  }
  e->value = std::to_string(next);
  *result = next;
  return IncrStatus::kOk;
}

bool Store::cron() {
  bool more = false;
  if (!heap_.empty()) {
    const int64_t now = now_ms();
    size_t budget = kSweepBudget;
    while (!heap_.empty() && heap_.front().when < now) {
      if (budget-- == 0) {
        more = true;
        break;
      }
      std::pop_heap(heap_.begin(), heap_.end(), later<HeapItem>);
      HeapItem item = std::move(heap_.back());
      heap_.pop_back();
      Entry* e = table_->find(item.key);
      if (e != nullptr && e->expire_at_ms == item.when) erase_entry(item.key, e);
    }
    if (heap_.empty() && heap_.capacity() > 4096) std::vector<HeapItem>().swap(heap_);
  }
  if (table_->rehash_some(kRehashBuckets)) more = true;
  return more;
}

}  // namespace engine
