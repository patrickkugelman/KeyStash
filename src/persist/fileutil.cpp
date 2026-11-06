#include "persist/fileutil.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "persist/crc32.h"

namespace persist {

void fatal(const std::string& message) {
  std::fprintf(stderr, "fatal: persistence: %s\n", message.c_str());
  std::fflush(stderr);
  std::_Exit(1);
}

std::string errno_text() { return std::strerror(errno); }

bool write_all(int fd, const char* data, size_t n) {
  while (n > 0) {
    const ssize_t w = ::write(fd, data, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    data += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

void write_all_or_die(int fd, const char* data, size_t n, const char* what) {
  if (!write_all(fd, data, n)) fatal(std::string("write ") + what + ": " + errno_text());
}

void fdatasync_or_die(int fd, const char* what) {
  if (::fdatasync(fd) != 0) fatal(std::string("fdatasync ") + what + ": " + errno_text());
}

void fsync_dir_or_die(const std::string& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) fatal("open directory " + dir + ": " + errno_text());
  if (::fsync(fd) != 0) {
    const std::string why = errno_text();
    ::close(fd);
    fatal("fsync directory " + dir + ": " + why);
  }
  ::close(fd);
}

Reader::Reader(int fd, bool track_crc) : fd_(fd), track_crc_(track_crc), buf_(256 * 1024) {
  struct stat st {};
  if (::fstat(fd, &st) != 0) throw std::runtime_error("fstat: " + errno_text());
  size_ = static_cast<uint64_t>(st.st_size);
}

bool Reader::ensure(size_t n) {
  while (end_ - pos_ < n) {
    if (pos_ > 0) {
      std::memmove(buf_.data(), buf_.data() + pos_, end_ - pos_);
      end_ -= pos_;
      pos_ = 0;
    }
    if (buf_.size() < n) buf_.resize(std::max(n, buf_.size() * 2));
    const ssize_t r = ::read(fd_, buf_.data() + end_, buf_.size() - end_);
    if (r == 0) return false;
    if (r < 0) {
      if (errno == EINTR) continue;
      throw std::runtime_error("read: " + errno_text());
    }
    end_ += static_cast<size_t>(r);
  }
  return true;
}

void Reader::skip(size_t n) {
  if (track_crc_) crc_ = crc32(data(), n, crc_);
  pos_ += n;
  consumed_ += n;
}

}  // namespace persist
