#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace persist {

// Logs and exits. A failed write or fsync can lose data silently (the kernel
// may drop dirty pages after a failed fsync), so the process never goes on.
[[noreturn]] void fatal(const std::string& message);

std::string errno_text();

// Writes everything or returns false with errno set.
bool write_all(int fd, const char* data, size_t n);
void write_all_or_die(int fd, const char* data, size_t n, const char* what);
void fdatasync_or_die(int fd, const char* what);
void fsync_dir_or_die(const std::string& dir);

// Buffered sequential reader. Memory use is bounded by the largest
// ensure() request, so callers must validate sizes before asking.
class Reader {
 public:
  Reader(int fd, bool track_crc);

  // Buffers at least n bytes; false if the file ends first. Throws on I/O error.
  bool ensure(size_t n);
  const char* data() const { return buf_.data() + pos_; }
  size_t avail() const { return end_ - pos_; }
  void skip(size_t n);

  uint64_t offset() const { return consumed_; }
  uint64_t file_size() const { return size_; }
  // CRC of everything skipped so far (only with track_crc).
  uint32_t crc() const { return crc_; }

 private:
  int fd_;
  bool track_crc_;
  std::vector<char> buf_;
  size_t pos_ = 0;
  size_t end_ = 0;
  uint64_t consumed_ = 0;
  uint64_t size_ = 0;
  uint32_t crc_ = 0;
};

}  // namespace persist
