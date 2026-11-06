#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace persist {

enum class FsyncPolicy { kAlways, kEverySec, kNo };

bool parse_fsync_policy(std::string_view s, FsyncPolicy* out);

// File layout: 16-byte header ("KVWAL001" + u64 sequence), then records:
//   u32 payload_len | u32 crc32(len bytes + payload) | payload
// payload = u8 op, u32 klen, key, then per op:
//   kSet / kSetKeepTtl: u32 vlen, value     kDel: nothing     kExpireAt: i64 when_ms
enum class OpType : uint8_t { kSet = 1, kSetKeepTtl = 2, kDel = 3, kExpireAt = 4 };

inline constexpr size_t kWalHeaderSize = 16;
inline constexpr char kWalMagic[8] = {'K', 'V', 'W', 'A', 'L', '0', '0', '1'};
// Two 512 MiB bulks plus framing.
inline constexpr size_t kMaxRecordPayload = (size_t{1} << 30) + 64;

std::string wal_path(const std::string& dir, uint64_t seq);

struct DecodedOp {
  OpType type;
  std::string_view key;
  std::string_view value;
  int64_t when_ms;
};

// False if the payload is malformed.
bool decode_op(std::string_view payload, DecodedOp* op);

// Appends records to one WAL file. Records are buffered in memory until
// commit(), which writes them in a single write() and, under `always`,
// fdatasyncs, so one commit per event-loop batch is a group commit.
class WalWriter {
 public:
  WalWriter() = default;
  ~WalWriter() { close(); }
  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // Opens `seq` for appending, creating it with a header if absent. A new
  // file is normally made durable (fdatasync + directory fsync) before this
  // returns; with `defer_creation_sync` that is left to the caller, see
  // creation_sync_pending(). Throws std::runtime_error if it cannot.
  void open(const std::string& dir, uint64_t seq, FsyncPolicy policy,
            bool defer_creation_sync = false);
  // Switches to a fresh file `new_seq`. On success the old file is NOT closed:
  // the caller gets its fd (and whether it still needs an fdatasync) and must
  // sync/close it, possibly on another thread. Returns false (nothing
  // changed) on failure.
  bool rotate(uint64_t new_seq, int* old_fd, bool* old_unsynced, std::string* error);
  void close();

  void append_set(std::string_view key, std::string_view value, bool keep_ttl);
  void append_del(std::string_view key);
  void append_expire_at(std::string_view key, int64_t when_ms);

  // Writes buffered records; fdatasyncs under `always`.
  void commit();
  // fdatasync now, on the calling thread, if anything written is not yet synced.
  void sync();
  // `everysec`: true when written data has been unsynced for over a second.
  bool sync_due() const;
  // Marks everything written so far as being synced and returns a dup of the
  // fd. The caller must fdatasync and close it (on any thread); the dup keeps
  // it valid even if this writer rotates meanwhile.
  int begin_background_sync();

  // True after a deferred creation: the directory entry still needs an fsync
  // of the directory. The file's data is covered by the normal sync policy.
  bool creation_sync_pending() const { return creation_pending_; }
  void clear_creation_sync_pending() { creation_pending_ = false; }

  uint64_t seq() const { return seq_; }
  uint64_t size() const { return file_size_ + buf_.size(); }

 private:
  void append(OpType type, std::string_view key, std::string_view value, int64_t when_ms);

  std::string dir_;
  FsyncPolicy policy_ = FsyncPolicy::kEverySec;
  int fd_ = -1;
  uint64_t seq_ = 0;
  uint64_t file_size_ = 0;
  bool unsynced_ = false;
  bool creation_pending_ = false;
  std::string buf_;
  std::chrono::steady_clock::time_point last_sync_{};
};

}  // namespace persist
