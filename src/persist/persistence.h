#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "engine/store.h"
#include "persist/snapshot.h"
#include "persist/wal.h"
#include "persist/worker.h"

namespace persist {

struct PersistStats {
  FsyncPolicy policy;
  uint64_t wal_seq;
  uint64_t wal_bytes;
  bool snapshot_in_progress;
  uint64_t snapshots_completed;
  uint64_t snapshot_failures;
  uint64_t last_snapshot_bytes;
  uint64_t last_capture_us;   // event loop pause for the last snapshot
  uint64_t last_write_ms;     // background write time of the last snapshot
  uint64_t background_syncs;  // fdatasyncs done off the event loop
};

// Owns the data directory: WAL files, snapshot, and the directory lock.
//   wal-<seq>.log   append-only log, rotated by each snapshot
//   snapshot.bin    full state as of the rotation that created wal-<next_wal_seq>
// Recovery = snapshot + every WAL file with seq >= next_wal_seq, in order.
//
// Threading: every public method runs on the event-loop thread. Two worker
// threads do blocking disk work and never touch the store or the WAL writer:
//   sync worker      fdatasync for `everysec` and for rotated-out WAL files
//   snapshot worker  writes snapshot.bin and then deletes the WAL files it covers
// They are separate so a slow snapshot write cannot delay the 1 s fsync.
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

  // Makes everything logged so far durable per the fsync policy
  // (`always`: fdatasync here; otherwise just write to the OS).
  void commit() { wal_.commit(); }

  // Periodic work: schedules the `everysec` fdatasync, collects finished
  // snapshots, starts automatic ones.
  void cron(engine::Store& store);

  // SAVE: capture, write and truncate before returning. The event loop is
  // blocked for the whole time.
  bool snapshot(engine::Store& store, std::string* error);

  // BGSAVE: the event loop only rotates the WAL and captures the data in
  // memory; the snapshot worker writes it out.
  bool start_background_snapshot(engine::Store& store, std::string* error);

  PersistStats stats();

 private:
  // Rotates the WAL and captures the store. Fails if a snapshot is running.
  bool begin_snapshot(engine::Store& store, SnapshotBlob* blob, std::string* error);
  void retire_wal_fd(int fd, bool unsynced);
  void delete_wal_files_before(uint64_t seq);
  void collect_finished_snapshot();

  struct SnapshotResult {
    bool ok = false;
    std::string error;
    uint64_t file_bytes = 0;
    uint64_t write_ms = 0;
  };

  std::string dir_;
  FsyncPolicy policy_;
  uint64_t snapshot_min_wal_bytes_;
  int lock_fd_ = -1;
  WalWriter wal_;
  uint64_t snapshot_bytes_ = 0;
  std::chrono::steady_clock::time_point retry_after_{};

  std::unique_ptr<Worker> sync_worker_;
  std::unique_ptr<Worker> snapshot_worker_;
  std::atomic<bool> sync_in_flight_{false};
  std::atomic<uint64_t> background_syncs_{0};

  bool snapshot_in_progress_ = false;           // event-loop thread only
  std::atomic<bool> snapshot_done_{false};      // set by the worker, cleared by the loop
  std::mutex result_mutex_;
  SnapshotResult result_;                       // guarded by result_mutex_
  uint64_t snapshots_completed_ = 0;
  uint64_t snapshot_failures_ = 0;
  uint64_t last_snapshot_bytes_ = 0;
  uint64_t last_capture_us_ = 0;
  uint64_t last_write_ms_ = 0;
};

}  // namespace persist
