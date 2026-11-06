#include "persist/wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "persist/codec.h"
#include "persist/crc32.h"
#include "persist/fileutil.h"

namespace persist {

bool parse_fsync_policy(std::string_view s, FsyncPolicy* out) {
  if (s == "always") {
    *out = FsyncPolicy::kAlways;
  } else if (s == "everysec") {
    *out = FsyncPolicy::kEverySec;
  } else if (s == "no") {
    *out = FsyncPolicy::kNo;
  } else {
    return false;
  }
  return true;
}

std::string wal_path(const std::string& dir, uint64_t seq) {
  char name[48];
  std::snprintf(name, sizeof(name), "/wal-%020llu.log", static_cast<unsigned long long>(seq));
  return dir + name;
}

bool decode_op(std::string_view p, DecodedOp* op) {
  if (p.size() < 5) return false;
  const auto type = static_cast<uint8_t>(p[0]);
  if (type < 1 || type > 4) return false;
  const size_t klen = get_u32(p.data() + 1);
  size_t pos = 5;
  if (p.size() - pos < klen) return false;
  op->type = static_cast<OpType>(type);
  op->key = p.substr(pos, klen);
  op->value = {};
  op->when_ms = 0;
  pos += klen;
  switch (op->type) {
    case OpType::kSet:
    case OpType::kSetKeepTtl: {
      if (p.size() - pos < 4) return false;
      const size_t vlen = get_u32(p.data() + pos);
      pos += 4;
      if (p.size() - pos != vlen) return false;
      op->value = p.substr(pos, vlen);
      return true;
    }
    case OpType::kDel:
      return pos == p.size();
    case OpType::kExpireAt:
      if (p.size() - pos != 8) return false;
      op->when_ms = static_cast<int64_t>(get_u64(p.data() + pos));
      return true;
  }
  return false;
}

void WalWriter::open(const std::string& dir, uint64_t seq, FsyncPolicy policy,
                     bool defer_creation_sync) {
  close();
  const std::string path = wal_path(dir, seq);
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) throw std::runtime_error("open " + path + ": " + errno_text());
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    const std::string why = errno_text();
    ::close(fd);
    throw std::runtime_error("fstat " + path + ": " + why);
  }
  uint64_t size = static_cast<uint64_t>(st.st_size);
  bool unsynced = false;
  bool creation_pending = false;
  if (size == 0) {
    std::string header(kWalMagic, sizeof(kWalMagic));
    put_u64(header, seq);
    const bool written = write_all(fd, header.data(), header.size());
    if (!written || (!defer_creation_sync && ::fdatasync(fd) != 0)) {
      const std::string why = errno_text();
      ::close(fd);
      throw std::runtime_error("write header " + path + ": " + why);
    }
    if (defer_creation_sync) {
      unsynced = true;  // the periodic sync gets the header
      creation_pending = true;
    } else {
      fsync_dir_or_die(dir);  // so the new file survives a crash
    }
    size = header.size();
  }
  dir_ = dir;
  policy_ = policy;
  fd_ = fd;
  seq_ = seq;
  file_size_ = size;
  unsynced_ = unsynced;
  creation_pending_ = creation_pending;
  buf_.clear();
  last_sync_ = std::chrono::steady_clock::now();
}

bool WalWriter::rotate(uint64_t new_seq, int* old_fd, bool* old_unsynced, std::string* error) {
  commit();
  WalWriter next;
  try {
    // always: the file has to be durable before any ack, so sync inline.
    // otherwise the caller queues the dir fsync on a worker
    next.open(dir_, new_seq, policy_, policy_ != FsyncPolicy::kAlways);
  } catch (const std::exception& e) {
    *error = e.what();
    return false;
  }
  *old_fd = fd_;
  *old_unsynced = unsynced_;
  fd_ = next.fd_;
  seq_ = next.seq_;
  file_size_ = next.file_size_;
  unsynced_ = next.unsynced_;
  creation_pending_ = next.creation_pending_;
  last_sync_ = next.last_sync_;
  next.fd_ = -1;  // moved out, keep the destructor from closing it
  return true;
}

void WalWriter::close() {
  if (fd_ < 0) return;
  commit();
  sync();
  ::close(fd_);
  fd_ = -1;
}

void WalWriter::append(OpType type, std::string_view key, std::string_view value,
                       int64_t when_ms) {
  const size_t start = buf_.size();
  buf_.append(8, '\0');  // len + crc, filled in at the end
  buf_.push_back(static_cast<char>(type));
  put_u32(buf_, static_cast<uint32_t>(key.size()));
  buf_.append(key);
  if (type == OpType::kSet || type == OpType::kSetKeepTtl) {
    put_u32(buf_, static_cast<uint32_t>(value.size()));
    buf_.append(value);
  } else if (type == OpType::kExpireAt) {
    put_u64(buf_, static_cast<uint64_t>(when_ms));
  }
  const size_t payload = buf_.size() - start - 8;
  if (payload > kMaxRecordPayload) fatal("record too large");
  store_u32(&buf_[start], static_cast<uint32_t>(payload));
  uint32_t crc = crc32(&buf_[start], 4);
  crc = crc32(&buf_[start + 8], payload, crc);
  store_u32(&buf_[start + 4], crc);
}

void WalWriter::append_set(std::string_view key, std::string_view value, bool keep_ttl) {
  append(keep_ttl ? OpType::kSetKeepTtl : OpType::kSet, key, value, 0);
}

void WalWriter::append_del(std::string_view key) { append(OpType::kDel, key, {}, 0); }

void WalWriter::append_expire_at(std::string_view key, int64_t when_ms) {
  append(OpType::kExpireAt, key, {}, when_ms);
}

void WalWriter::commit() {
  if (fd_ < 0) return;
  if (!buf_.empty()) {
    write_all_or_die(fd_, buf_.data(), buf_.size(), "WAL");
    file_size_ += buf_.size();
    unsynced_ = true;
    buf_.clear();
    if (buf_.capacity() > (size_t{64} << 20)) std::string().swap(buf_);
  }
  if (policy_ == FsyncPolicy::kAlways) sync();
}

void WalWriter::sync() {
  if (fd_ < 0 || !unsynced_) return;
  fdatasync_or_die(fd_, "WAL");
  unsynced_ = false;
  last_sync_ = std::chrono::steady_clock::now();
}

bool WalWriter::sync_due() const {
  return policy_ == FsyncPolicy::kEverySec && unsynced_ &&
         std::chrono::steady_clock::now() - last_sync_ >= std::chrono::seconds(1);
}

int WalWriter::begin_background_sync() {
  const int copy = ::dup(fd_);
  if (copy < 0) fatal("dup WAL fd: " + errno_text());
  unsynced_ = false;
  last_sync_ = std::chrono::steady_clock::now();
  return copy;
}

}  // namespace persist
