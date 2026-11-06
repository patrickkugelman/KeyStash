#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace persist {

// log and exit. after a failed write/fsync the kernel may drop dirty pages
// without telling us, so carrying on isn't safe
[[noreturn]] void fatal(const std::string& message);

std::string errno_text();

bool write_all(int fd, const char* data, size_t n);
void write_all_or_die(int fd, const char* data, size_t n, const char* what);
void fdatasync_or_die(int fd, const char* what);
void fsync_dir_or_die(const std::string& dir);

// buffered sequential reader, memory follows the biggest ensure() so check
// sizes before calling it
class Reader {
 public:
  Reader(int fd, bool track_crc);

  // at least n bytes buffered, false at EOF
  bool ensure(size_t n);
  const char* data() const { return buf_.data() + pos_; }
  size_t avail() const { return end_ - pos_; }
  void skip(size_t n);

  uint64_t offset() const { return consumed_; }
  uint64_t file_size() const { return size_; }
  // crc of everything skipped so far, needs track_crc
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
