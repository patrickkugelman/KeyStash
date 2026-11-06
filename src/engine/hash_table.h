#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "engine/table.h"

namespace engine {

// Chained hash table with two bucket arrays and incremental rehashing: a
// resize moves a few buckets per operation instead of stalling on one big
// copy. Nodes are relinked, never copied, so Entry pointers stay stable.
class HashTable : public Table {
 public:
  HashTable() = default;
  ~HashTable() override;
  HashTable(const HashTable&) = delete;
  HashTable& operator=(const HashTable&) = delete;

  Entry* find(std::string_view key) override;
  Entry* find_or_insert(std::string_view key, bool* inserted) override;
  bool erase(std::string_view key) override;
  size_t size() const override { return tables_[0].used + tables_[1].used; }
  bool rehash_some(size_t buckets) override { return rehash_step(buckets); }

 private:
  struct Node {
    Node* next;
    size_t hash;
    std::string key;
    Entry entry;
  };
  struct Buckets {
    std::unique_ptr<Node*[]> slots;
    size_t size = 0;  // power of two, or 0 when unallocated
    size_t used = 0;
  };

  static void allocate(Buckets& b, size_t n);
  static void destroy(Buckets& b);
  Node* lookup(std::string_view key, size_t hash) const;
  void maybe_shrink();
  void start_rehash(size_t new_size);
  bool rehash_step(size_t buckets);

  Buckets tables_[2];  // [1] is the rehash target while rehashing_
  size_t rehash_idx_ = 0;  // next bucket of tables_[0] to move
  bool rehashing_ = false;
};

}  // namespace engine
