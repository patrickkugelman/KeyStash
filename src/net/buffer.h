#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <string_view>

namespace net {

// byte fifo, append() invalidates data()
class Buffer {
 public:
  const char* data() const { return storage_.get() + read_; }
  size_t size() const { return write_ - read_; }
  bool empty() const { return read_ == write_; }

  void append(const char* src, size_t n) {
    if (n == 0) return;
    ensure_tail(n);
    std::memcpy(storage_.get() + write_, src, n);
    write_ += n;
  }
  void append(std::string_view s) { append(s.data(), s.size()); }

  void consume(size_t n) {
    read_ += std::min(n, size());
    if (read_ == write_) {
      read_ = write_ = 0;
      // don't hold on to a huge buffer after one big request
      if (capacity_ > kRetainMax) {
        storage_.reset();
        capacity_ = 0;
      }
    }
  }

  void clear() { consume(size()); }

 private:
  static constexpr size_t kRetainMax = 1024 * 1024;
  static constexpr size_t kMinCapacity = 256;

  void ensure_tail(size_t n) {
    if (capacity_ - write_ >= n) return;
    const size_t live = size();
    // only compact when it frees at least half
    if (live + n <= capacity_ && read_ >= capacity_ / 2) {
      std::memmove(storage_.get(), storage_.get() + read_, live);
    } else {
      const size_t cap = std::max({capacity_ * 2, live + n, kMinCapacity});
      std::unique_ptr<char[]> grown(new char[cap]);
      if (live != 0) std::memcpy(grown.get(), storage_.get() + read_, live);
      storage_ = std::move(grown);
      capacity_ = cap;
    }
    read_ = 0;
    write_ = live;
  }

  std::unique_ptr<char[]> storage_;
  size_t capacity_ = 0;
  size_t read_ = 0;
  size_t write_ = 0;
};

}  // namespace net
