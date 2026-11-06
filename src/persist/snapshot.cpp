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

constexpr size_t kChunkSize = 1 << 20;

}  // namespace

SnapshotBlob capture_snapshot(engine::Store& store, uint64_t next_wal_seq) {
  SnapshotBlob blob;
  blob.next_wal_seq = next_wal_seq;

  std::string chunk;
  chunk.reserve(kChunkSize + 4096);
  chunk.append(kSnapshotMagic, sizeof(kSnapshotMagic));
  put_u64(chunk, next_wal_seq);

  store.for_each_live([&](std::string_view key, const engine::Entry& entry) {
    put_u32(chunk, static_cast<uint32_t>(key.size()));
    chunk.append(key);
    put_u32(chunk, static_cast<uint32_t>(entry.value.size()));
    chunk.append(entry.value);
    put_u64(chunk, static_cast<uint64_t>(entry.expire_at_ms));
    ++blob.count;
    if (chunk.size() >= kChunkSize) {
      blob.bytes += chunk.size();
      blob.chunks.push_back(std::move(chunk));
      chunk = std::string();
      chunk.reserve(kChunkSize + 4096);
    }
  });
  blob.bytes += chunk.size();
  blob.chunks.push_back(std::move(chunk));
  return blob;
}

bool write_snapshot(const SnapshotBlob& blob, const std::string& dir, uint64_t* file_bytes,
                    std::string* error) {
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

  uint32_t crc = 0;
  for (const std::string& chunk : blob.chunks) {
    crc = crc32(chunk.data(), chunk.size(), crc);
    if (!write_all(fd, chunk.data(), chunk.size())) return fail("write snapshot");
  }
  std::string footer;
  put_u32(footer, kSnapshotFooterMarker);
  put_u64(footer, blob.count);
  crc = crc32(footer.data(), footer.size(), crc);  // marker and count are covered too
  put_u32(footer, crc);
  if (!write_all(fd, footer.data(), footer.size())) return fail("write snapshot");

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
  *file_bytes = blob.bytes + footer.size();
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
