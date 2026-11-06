#include "persist/snapshot.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <stdexcept>

#include "persist/codec.h"
#include "persist/crc32.h"
#include "persist/fileutil.h"

namespace persist {
namespace {

constexpr size_t kFlushAt = 1 << 20;

// Buffered output that keeps a running CRC of what it was given.
class SnapshotOut {
 public:
  explicit SnapshotOut(int fd) : fd_(fd) { buf_.reserve(kFlushAt + 4096); }

  void put(const char* p, size_t n) {
    crc_ = crc32(p, n, crc_);
    buf_.append(p, n);
  }
  void put_u32v(uint32_t v) {
    std::string s;
    put_u32(s, v);
    put(s.data(), s.size());
  }
  void put_u64v(uint64_t v) {
    std::string s;
    put_u64(s, v);
    put(s.data(), s.size());
  }
  // Appends without checksumming (the CRC field itself).
  void put_raw_u32(uint32_t v) { put_u32(buf_, v); }

  bool maybe_flush() { return buf_.size() < kFlushAt || flush(); }
  bool flush() {
    if (!write_all(fd_, buf_.data(), buf_.size())) return false;
    total_ += buf_.size();
    buf_.clear();
    return true;
  }
  uint32_t crc() const { return crc_; }
  uint64_t total() const { return total_; }

 private:
  int fd_;
  std::string buf_;
  uint32_t crc_ = 0;
  uint64_t total_ = 0;
};

}  // namespace

bool write_snapshot(engine::Store& store, const std::string& dir, uint64_t next_wal_seq,
                    uint64_t* bytes, std::string* error) {
  const std::string tmp = dir + "/snapshot.tmp";
  const std::string final_path = dir + "/snapshot.bin";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    *error = "open " + tmp + ": " + errno_text();
    return false;
  }
  auto fail = [&](const std::string& what) {
    *error = what + ": " + errno_text();
    ::close(fd);
    ::unlink(tmp.c_str());
    return false;
  };

  SnapshotOut out(fd);
  out.put(kSnapshotMagic, sizeof(kSnapshotMagic));
  out.put_u64v(next_wal_seq);
  uint64_t count = 0;
  bool io_ok = true;
  store.for_each_live([&](std::string_view key, const engine::Entry& entry) {
    if (!io_ok) return;
    out.put_u32v(static_cast<uint32_t>(key.size()));
    out.put(key.data(), key.size());
    out.put_u32v(static_cast<uint32_t>(entry.value.size()));
    out.put(entry.value.data(), entry.value.size());
    out.put_u64v(static_cast<uint64_t>(entry.expire_at_ms));
    ++count;
    io_ok = out.maybe_flush();
  });
  if (!io_ok) return fail("write snapshot");

  out.put_u32v(kSnapshotFooterMarker);
  out.put_u64v(count);
  out.put_raw_u32(out.crc());
  if (!out.flush()) return fail("write snapshot");
  if (::fsync(fd) != 0) return fail("fsync snapshot");
  if (::close(fd) != 0) {
    *error = "close snapshot: " + errno_text();
    ::unlink(tmp.c_str());
    return false;
  }
  if (::rename(tmp.c_str(), final_path.c_str()) != 0) {
    *error = "rename snapshot: " + errno_text();
    ::unlink(tmp.c_str());
    return false;
  }
  fsync_dir_or_die(dir);
  *bytes = out.total();
  return true;
}

void load_snapshot(const std::string& path, engine::Store& store, uint64_t* next_wal_seq,
                   uint64_t* entries) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) throw std::runtime_error("open " + path + ": " + errno_text());
  struct Closer {
    int fd;
    ~Closer() { ::close(fd); }
  } closer{fd};

  auto corrupt = [&](const std::string& why) {
    return std::runtime_error("snapshot " + path + " is damaged: " + why);
  };

  Reader in(fd, true);
  if (!in.ensure(16)) throw corrupt("file too short");
  if (std::string_view(in.data(), 8) != std::string_view(kSnapshotMagic, 8)) {
    throw corrupt("bad magic");
  }
  *next_wal_seq = get_u64(in.data() + 8);
  in.skip(16);

  uint64_t count = 0;
  while (true) {
    if (!in.ensure(4)) throw corrupt("truncated");
    const uint32_t klen = get_u32(in.data());
    if (klen == kSnapshotFooterMarker) break;
    if (klen > kMaxSnapshotString) throw corrupt("key length out of range");
    if (in.offset() + 4 + klen + 4 > in.file_size()) throw corrupt("truncated entry");
    if (!in.ensure(size_t{4} + klen + 4)) throw corrupt("truncated entry");
    const uint32_t vlen = get_u32(in.data() + 4 + klen);
    if (vlen > kMaxSnapshotString) throw corrupt("value length out of range");
    const size_t total = size_t{4} + klen + 4 + vlen + 8;
    if (in.offset() + total > in.file_size()) throw corrupt("truncated entry");
    if (!in.ensure(total)) throw corrupt("truncated entry");
    const char* p = in.data();
    const int64_t expire = static_cast<int64_t>(get_u64(p + total - 8));
    store.load_entry(std::string_view(p + 4, klen), std::string_view(p + 8 + klen, vlen), expire);
    in.skip(total);
    ++count;
  }

  if (!in.ensure(12)) throw corrupt("truncated footer");
  const uint64_t declared = get_u64(in.data() + 4);
  in.skip(12);
  const uint32_t computed = in.crc();
  if (!in.ensure(4)) throw corrupt("truncated footer");
  if (get_u32(in.data()) != computed) throw corrupt("checksum mismatch");
  in.skip(4);
  if (in.ensure(1)) throw corrupt("trailing bytes after footer");
  if (declared != count) throw corrupt("entry count mismatch");
  *entries = count;
}

}  // namespace persist
