#include "engine/hash_table.h"

#include <functional>
#include <utility>

namespace engine {
namespace {

constexpr size_t kInitialSize = 8;

size_t next_pow2(size_t n) {
  size_t p = kInitialSize;
  while (p < n) p <<= 1;
  return p;
}

size_t hash_of(std::string_view key) { return std::hash<std::string_view>{}(key); }

}  // namespace

void HashTable::allocate(Buckets& b, size_t n) {
  b.slots.reset(new Node*[n]());
  b.size = n;
  b.used = 0;
}

void HashTable::destroy(Buckets& b) {
  for (size_t i = 0; i < b.size; ++i) {
    for (Node* n = b.slots[i]; n != nullptr;) {
      Node* next = n->next;
      delete n;
      n = next;
    }
  }
  b = Buckets{};
}

HashTable::~HashTable() {
  destroy(tables_[0]);
  destroy(tables_[1]);
}

HashTable::Node* HashTable::lookup(std::string_view key, size_t hash) const {
  for (int t = 0; t <= (rehashing_ ? 1 : 0); ++t) {
    const Buckets& b = tables_[t];
    if (b.size == 0) continue;
    for (Node* n = b.slots[hash & (b.size - 1)]; n != nullptr; n = n->next) {
      if (n->hash == hash && n->key == key) return n;
    }
  }
  return nullptr;
}

Entry* HashTable::find(std::string_view key) {
  rehash_step(1);
  Node* n = lookup(key, hash_of(key));
  return n == nullptr ? nullptr : &n->entry;
}

Entry* HashTable::find_or_insert(std::string_view key, bool* inserted) {
  rehash_step(1);
  const size_t hash = hash_of(key);
  if (Node* existing = lookup(key, hash)) {
    *inserted = false;
    return &existing->entry;
  }
  if (!rehashing_) {
    if (tables_[0].size == 0) {
      allocate(tables_[0], kInitialSize);
    } else if (tables_[0].used >= tables_[0].size) {
      start_rehash(next_pow2(tables_[0].used * 2));
    }
  }
  // New keys go to the target table so the old one only drains.
  Buckets& dst = rehashing_ ? tables_[1] : tables_[0];
  Node*& head = dst.slots[hash & (dst.size - 1)];
  head = new Node{head, hash, std::string(key), Entry{}};
  ++dst.used;
  *inserted = true;
  return &head->entry;
}

bool HashTable::erase(std::string_view key) {
  rehash_step(1);
  const size_t hash = hash_of(key);
  for (int t = 0; t <= (rehashing_ ? 1 : 0); ++t) {
    Buckets& b = tables_[t];
    if (b.size == 0) continue;
    for (Node** link = &b.slots[hash & (b.size - 1)]; *link != nullptr; link = &(*link)->next) {
      Node* n = *link;
      if (n->hash != hash || n->key != key) continue;
      *link = n->next;
      delete n;
      --b.used;
      maybe_shrink();
      return true;
    }
  }
  return false;
}

// Below 1/8 load, shrink to 2x the live count so a refill does not regrow at once.
void HashTable::maybe_shrink() {
  if (!rehashing_ && tables_[0].size > kInitialSize && tables_[0].used * 8 < tables_[0].size) {
    start_rehash(next_pow2(tables_[0].used * 2));
  }
}

void HashTable::start_rehash(size_t new_size) {
  allocate(tables_[1], new_size);
  rehash_idx_ = 0;
  rehashing_ = true;
}

bool HashTable::rehash_step(size_t buckets) {
  if (!rehashing_) return false;
  Buckets& from = tables_[0];
  Buckets& to = tables_[1];
  size_t empty_visits = buckets * 10;  // bounds the work spent skipping holes
  while (buckets > 0 && from.used > 0) {
    while (from.slots[rehash_idx_] == nullptr) {
      ++rehash_idx_;
      if (--empty_visits == 0) return true;
    }
    for (Node* n = from.slots[rehash_idx_]; n != nullptr;) {
      Node* next = n->next;
      Node*& head = to.slots[n->hash & (to.size - 1)];
      n->next = head;
      head = n;
      --from.used;
      ++to.used;
      n = next;
    }
    from.slots[rehash_idx_] = nullptr;
    ++rehash_idx_;
    --buckets;
  }
  if (from.used != 0) return true;
  tables_[0] = std::move(tables_[1]);
  tables_[1] = Buckets{};
  rehashing_ = false;
  rehash_idx_ = 0;
  // Deletes during the rehash may have left the new table too big already.
  maybe_shrink();
  return rehashing_;
}

}  // namespace engine
