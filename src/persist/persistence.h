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
  uint64_t last_capture_us;   // how long the loop was paused
  uint64_t last_write_ms;     // how long the worker took to write
  uint64_t background_syncs;  // fdatasyncs done by the sync worker
};

// the data dir: wal-<seq>.log files and snapshot.bin
// snapshot.bin covers every wal before wal-<next_wal_seq>
// recovery = snapshot + the wal files from next_wal_seq on
//
// public methods run on the loop thread only. two workers do the blocking
// disk work and never touch the store or the wal writer:
//   sync worker      fdatasync for everysec and for rotated-out wal files
//   snapshot worker  writes snapshot.bin, then deletes the wal files it covers
// separate threads so a slow snapshot can't hold up the 1s fsync
class Persistence {
 public:
  Persistence(std::string dir, FsyncPolicy policy, uint64_t snapshot_min_wal_bytes);
  ~Persistence();
  Persistence(const Persistence&) = delete;
  Persistence& operator=(const Persistence&) = delete;

  // lock the dir, load the snapshot, replay the wal (a torn last record is
  // cut off), open the wal for appending. throws if it can't recover safely
  void recover(engine::Store& store);

  void log_set(std::string_view key, std::string_view value) { wal_.append_set(key, value, false); }
  void log_set_keep_ttl(std::string_view key, std::string_view value) {
    wal_.append_set(key, value, true);
  }
  void log_del(std::string_view key) { wal_.append_del(key); }
  void log_expire_at(std::string_view key, int64_t when_ms) { wal_.append_expire_at(key, when_ms); }

  // always: write + fdatasync here, otherwise just write to the OS
  void commit() { wal_.commit(); }

  // everysec sync, finished snapshots, automatic snapshots
  void cron(engine::Store& store);

  // SAVE, blocks the loop until it is done
  bool snapshot(engine::Store& store, std::string* error);

  // BGSAVE, the loop rotates the wal and copies the data, the worker writes it
  bool start_background_snapshot(engine::Store& store, std::string* error);

  PersistStats stats();

 private:
  // rotate + capture, fails if a snapshot is already running
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

  bool snapshot_in_progress_ = false;           // loop thread only
  std::atomic<bool> snapshot_done_{false};      // set by the worker
  std::mutex result_mutex_;
  SnapshotResult result_;                       // under result_mutex_
  uint64_t snapshots_completed_ = 0;
  uint64_t snapshot_failures_ = 0;
  uint64_t last_snapshot_bytes_ = 0;
  uint64_t last_capture_us_ = 0;
  uint64_t last_write_ms_ = 0;
};

}  // namespace persist
