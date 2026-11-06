#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace persist {

enum class FsyncPolicy { kAlways, kEverySec, kNo };

bool parse_fsync_policy(std::string_view s, FsyncPolicy* out);

// file: 16 byte header ("KVWAL001" + u64 seq), then records
//   u32 payload_len | u32 crc32(len bytes + payload) | payload
// payload: u8 op, u32 klen, key, then
//   set / set-keep-ttl: u32 vlen, value    del: nothing    expire-at: i64 ms
enum class OpType : uint8_t { kSet = 1, kSetKeepTtl = 2, kDel = 3, kExpireAt = 4 };

inline constexpr size_t kWalHeaderSize = 16;
inline constexpr char kWalMagic[8] = {'K', 'V', 'W', 'A', 'L', '0', '0', '1'};
// two 512 MiB bulks plus framing
inline constexpr size_t kMaxRecordPayload = (size_t{1} << 30) + 64;

std::string wal_path(const std::string& dir, uint64_t seq);

struct DecodedOp {
  OpType type;
  std::string_view key;
  std::string_view value;
  int64_t when_ms;
};

// false if the payload is malformed
bool decode_op(std::string_view payload, DecodedOp* op);

// appends records to one wal file. they sit in memory until commit(), which
// does one write() and under always an fdatasync, so a commit per loop
// batch is a group commit
class WalWriter {
 public:
  WalWriter() = default;
  ~WalWriter() { close(); }
  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // open for append, create with a header if missing. a new file is synced
  // (data + dir) before returning, unless defer_creation_sync: then the
  // caller does the dir fsync, see creation_sync_pending(). throws on failure
  void open(const std::string& dir, uint64_t seq, FsyncPolicy policy,
            bool defer_creation_sync = false);
  // switch to a new file. the old fd comes back unclosed, plus whether it
  // needs an fdatasync, so another thread can finish it. false = nothing changed
  bool rotate(uint64_t new_seq, int* old_fd, bool* old_unsynced, std::string* error);
  void close();

  void append_set(std::string_view key, std::string_view value, bool keep_ttl);
  void append_del(std::string_view key);
  void append_expire_at(std::string_view key, int64_t when_ms);

  // write the buffered records, fdatasync under always
  void commit();
  // fdatasync here if something is unsynced
  void sync();
  // everysec: unsynced data older than a second
  bool sync_due() const;
  // marks what was written as synced and returns a dup of the fd. the caller
  // fdatasyncs and closes it, any thread. the dup survives a rotate
  int begin_background_sync();

  // after a deferred create the directory still needs an fsync
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
