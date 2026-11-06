#pragma once

#include <cstdint>
#include <chrono>
#include <string>
#include <string_view>

#include "engine/store.h"
#include "persist/wal.h"

namespace persist {

// Owns the data directory: WAL files, snapshot, and the directory lock.
//   wal-<seq>.log   append-only log, rotated by each snapshot
//   snapshot.bin    full state as of the rotation that created wal-<next_wal_seq>
// Recovery = snapshot + every WAL file with seq >= next_wal_seq, in order.
class Persistence {
 public:
  Persistence(std::string dir, FsyncPolicy policy, uint64_t snapshot_min_wal_bytes);
  ~Persistence();
  Persistence(const Persistence&) = delete;
  Persistence& operator=(const Persistence&) = delete;

  // Locks the directory, loads the snapshot, replays the WAL (dropping a torn
  // final record) and opens the WAL for appending. Throws std::runtime_error
  // for anything it cannot safely recover from.
  void recover(engine::Store& store);

  void log_set(std::string_view key, std::string_view value) { wal_.append_set(key, value, false); }
  void log_set_keep_ttl(std::string_view key, std::string_view value) {
    wal_.append_set(key, value, true);
  }
  void log_del(std::string_view key) { wal_.append_del(key); }
  void log_expire_at(std::string_view key, int64_t when_ms) { wal_.append_expire_at(key, when_ms); }

  // Makes everything logged so far durable per the fsync policy.
  void commit() { wal_.commit(); }

  // Periodic work: `everysec` fsync and automatic snapshots.
  void cron(engine::Store& store);

  // Rotates the WAL, writes a snapshot and deletes the WAL files it covers.
  // Runs on the calling thread.
  bool snapshot(engine::Store& store, std::string* error);

 private:
  std::string dir_;
  FsyncPolicy policy_;
  uint64_t snapshot_min_wal_bytes_;
  int lock_fd_ = -1;
  WalWriter wal_;
  uint64_t snapshot_bytes_ = 0;
  std::chrono::steady_clock::time_point retry_after_{};
};

}  // namespace persist
