#include "persist/persistence.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <vector>

#include "persist/codec.h"
#include "persist/crc32.h"
#include "persist/fileutil.h"
#include "persist/snapshot.h"

namespace persist {
namespace {

std::vector<uint64_t> list_wal_seqs(const std::string& dir) {
  DIR* d = ::opendir(dir.c_str());
  if (d == nullptr) throw std::runtime_error("opendir " + dir + ": " + errno_text());
  std::vector<uint64_t> seqs;
  while (const dirent* e = ::readdir(d)) {
    const std::string name = e->d_name;
    // wal-<20 digits>.log
    if (name.size() != 28 || name.compare(0, 4, "wal-") != 0 || name.compare(24, 4, ".log") != 0) {
      continue;
    }
    const std::string digits = name.substr(4, 20);
    if (digits.find_first_not_of("0123456789") != std::string::npos) continue;
    seqs.push_back(std::strtoull(digits.c_str(), nullptr, 10));
  }
  ::closedir(d);
  std::sort(seqs.begin(), seqs.end());
  return seqs;
}

// True if a record with a valid checksum starts at `offset`.
bool valid_record_at(int fd, uint64_t offset, uint64_t file_size) {
  char head[8];
  if (offset + 8 > file_size || ::pread(fd, head, 8, static_cast<off_t>(offset)) != 8) return false;
  const uint32_t len = get_u32(head);
  if (len == 0 || len > kMaxRecordPayload || offset + 8 + len > file_size) return false;
  uint32_t crc = crc32(head, 4);
  std::vector<char> chunk(64 * 1024);
  uint64_t done = 0;
  while (done < len) {
    const size_t n = static_cast<size_t>(std::min<uint64_t>(chunk.size(), len - done));
    if (::pread(fd, chunk.data(), n, static_cast<off_t>(offset + 8 + done)) !=
        static_cast<ssize_t>(n)) {
      return false;
    }
    crc = crc32(chunk.data(), n, crc);
    done += n;
  }
  return crc == get_u32(head + 4);
}

struct ReplayResult {
  uint64_t records = 0;
  uint64_t good_end = 0;     // bytes of the file to keep
  uint64_t dropped = 0;      // torn tail bytes to cut off
  std::string torn_reason;   // empty if the file was clean
  bool header_missing = false;
};

void apply(engine::Store& store, const DecodedOp& op) {
  switch (op.type) {
    case OpType::kSet: store.set(op.key, op.value); break;
    case OpType::kSetKeepTtl: store.set_keep_ttl(op.key, op.value); break;
    case OpType::kDel: store.del(op.key); break;
    case OpType::kExpireAt: store.expire_at(op.key, op.when_ms); break;
  }
}

// Replays one WAL file. Only the final file may end in a torn record; any
// other damage throws, because continuing would silently lose data.
ReplayResult replay_file(const std::string& path, uint64_t seq, engine::Store& store,
                         bool is_last) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) throw std::runtime_error("open " + path + ": " + errno_text());
  struct Closer {
    int fd;
    ~Closer() { ::close(fd); }
  } closer{fd};

  Reader in(fd, false);
  const uint64_t size = in.file_size();
  ReplayResult result;

  if (!in.ensure(kWalHeaderSize)) {
    if (!is_last) throw std::runtime_error(path + ": header is truncated in a non-final WAL file");
    result.header_missing = true;  // crashed while creating it; nothing was logged
    result.dropped = size;
    return result;
  }
  if (std::string_view(in.data(), 8) != std::string_view(kWalMagic, 8) ||
      get_u64(in.data() + 8) != seq) {
    throw std::runtime_error(path + ": bad WAL header");
  }
  in.skip(kWalHeaderSize);

  while (in.offset() != size) {
    const uint64_t off = in.offset();
    auto torn = [&](const char* why) {
      if (!is_last) {
        throw std::runtime_error(path + ": " + why + " at offset " + std::to_string(off) +
                                 " in a non-final WAL file");
      }
      result.good_end = off;
      result.dropped = size - off;
      result.torn_reason = why;
      return result;
    };

    if (!in.ensure(8)) return torn("incomplete record header");
    const uint32_t len = get_u32(in.data());
    const uint32_t stored_crc = get_u32(in.data() + 4);
    if (len == 0 || len > kMaxRecordPayload) return torn("invalid record length");
    const uint64_t end = off + 8 + len;
    if (end > size) return torn("incomplete record");
    if (!in.ensure(size_t{8} + len)) throw std::runtime_error(path + ": file changed during read");

    uint32_t crc = crc32(in.data(), 4);
    crc = crc32(in.data() + 8, len, crc);
    if (crc != stored_crc) {
      // A tear damages only the end of the file. A valid record after the bad
      // one means the damage is in the middle, which we must not skip over.
      if (end < size && valid_record_at(fd, end, size)) {
        throw std::runtime_error(path + ": corrupt record at offset " + std::to_string(off) +
                                 " is followed by valid records; refusing to drop them");
      }
      return torn("checksum mismatch");
    }
    DecodedOp op;
    if (!decode_op(std::string_view(in.data() + 8, len), &op)) {
      throw std::runtime_error(path + ": malformed record with a valid checksum at offset " +
                               std::to_string(off));
    }
    apply(store, op);
    in.skip(size_t{8} + len);
    ++result.records;
  }
  result.good_end = size;
  return result;
}

}  // namespace

Persistence::Persistence(std::string dir, FsyncPolicy policy, uint64_t snapshot_min_wal_bytes)
    : dir_(std::move(dir)), policy_(policy), snapshot_min_wal_bytes_(snapshot_min_wal_bytes) {}

Persistence::~Persistence() {
  wal_.close();
  if (lock_fd_ >= 0) ::close(lock_fd_);
}

void Persistence::recover(engine::Store& store) {
  if (::mkdir(dir_.c_str(), 0755) != 0 && errno != EEXIST) {
    throw std::runtime_error("mkdir " + dir_ + ": " + errno_text());
  }
  lock_fd_ = ::open((dir_ + "/LOCK").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (lock_fd_ < 0) throw std::runtime_error("open lock file: " + errno_text());
  if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
    throw std::runtime_error("data directory " + dir_ + " is in use by another process");
  }
  ::unlink((dir_ + "/snapshot.tmp").c_str());  // an unfinished snapshot is garbage

  uint64_t next_seq = 1;
  uint64_t snapshot_entries = 0;
  const std::string snap_path = dir_ + "/snapshot.bin";
  const bool have_snapshot = ::access(snap_path.c_str(), F_OK) == 0;
  if (have_snapshot) {
    load_snapshot(snap_path, store, &next_seq, &snapshot_entries);
    struct stat st {};
    if (::stat(snap_path.c_str(), &st) == 0) snapshot_bytes_ = static_cast<uint64_t>(st.st_size);
  }

  std::vector<uint64_t> seqs = list_wal_seqs(dir_);
  size_t stale = 0;
  while (!seqs.empty() && seqs.front() < next_seq) {  // snapshot already covers them
    ::unlink(wal_path(dir_, seqs.front()).c_str());
    seqs.erase(seqs.begin());
    ++stale;
  }
  if (stale > 0) fsync_dir_or_die(dir_);
  if (!seqs.empty() && seqs.front() != next_seq && (have_snapshot || seqs.front() != 1)) {
    throw std::runtime_error("WAL history is incomplete: expected wal " + std::to_string(next_seq) +
                             " but the oldest is " + std::to_string(seqs.front()));
  }
  for (size_t i = 1; i < seqs.size(); ++i) {
    if (seqs[i] != seqs[i - 1] + 1) {
      throw std::runtime_error("WAL history has a gap between " + std::to_string(seqs[i - 1]) +
                               " and " + std::to_string(seqs[i]));
    }
  }

  uint64_t records = 0;
  uint64_t current = seqs.empty() ? next_seq : seqs.back();
  for (size_t i = 0; i < seqs.size(); ++i) {
    const std::string path = wal_path(dir_, seqs[i]);
    const bool last = i + 1 == seqs.size();
    const ReplayResult r = replay_file(path, seqs[i], store, last);
    records += r.records;
    if (r.header_missing) {
      if (::truncate(path.c_str(), 0) != 0) throw std::runtime_error("truncate " + path);
      std::fprintf(stderr, "recovery: %s had no complete header (%llu bytes); starting it over\n",
                   path.c_str(), static_cast<unsigned long long>(r.dropped));
    } else if (!r.torn_reason.empty()) {
      if (::truncate(path.c_str(), static_cast<off_t>(r.good_end)) != 0) {
        throw std::runtime_error("truncate " + path + ": " + errno_text());
      }
      const int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
      if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
      }
      std::fprintf(stderr, "recovery: dropped %llu bytes at the end of %s (%s)\n",
                   static_cast<unsigned long long>(r.dropped), path.c_str(), r.torn_reason.c_str());
    }
  }

  wal_.open(dir_, current, policy_);
  std::fprintf(stderr, "recovery: snapshot entries=%llu, wal files=%zu, wal records=%llu, keys=%zu\n",
               static_cast<unsigned long long>(snapshot_entries), seqs.size(),
               static_cast<unsigned long long>(records), store.size());
}

void Persistence::cron(engine::Store& store) {
  wal_.sync_if_due();
  const uint64_t threshold = std::max(snapshot_min_wal_bytes_, snapshot_bytes_);
  if (snapshot_min_wal_bytes_ == 0 || wal_.size() <= threshold) return;
  if (std::chrono::steady_clock::now() < retry_after_) return;
  std::string error;
  if (!snapshot(store, &error)) {
    std::fprintf(stderr, "snapshot failed: %s\n", error.c_str());
    retry_after_ = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  }
}

bool Persistence::snapshot(engine::Store& store, std::string* error) {
  wal_.commit();
  wal_.sync();
  // New writes go to a fresh file from here on; the snapshot covers all before it.
  const uint64_t new_seq = wal_.seq() + 1;
  if (!wal_.rotate(new_seq, error)) return false;

  uint64_t bytes = 0;
  if (!write_snapshot(store, dir_, new_seq, &bytes, error)) return false;
  snapshot_bytes_ = bytes;

  // Truncation: the snapshot is durable, so older WAL files are now redundant.
  for (const uint64_t seq : list_wal_seqs(dir_)) {
    if (seq < new_seq) ::unlink(wal_path(dir_, seq).c_str());
  }
  fsync_dir_or_die(dir_);
  return true;
}

}  // namespace persist
