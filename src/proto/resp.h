#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "net/buffer.h"

namespace proto {

// redis defaults
inline constexpr size_t kMaxArgs = 1024 * 1024;
inline constexpr size_t kMaxBulk = 512 * 1024 * 1024;
inline constexpr size_t kMaxLine = 64 * 1024;
inline constexpr size_t kMaxRequest = 1024 * 1024 * 1024;

// incremental request parser (multibulk and inline), args are views into
// the caller's buffer
class Parser {
 public:
  enum class Status { kNeedMore, kComplete, kError };

  // data = all unconsumed input, it can move between calls but only grows.
  // picks up where the last call stopped
  Status parse(const char* data, size_t len);

  // valid until the input buffer changes, empty for a blank line
  const std::vector<std::string_view>& args() const { return args_; }
  // bytes to drop from the input after kComplete
  size_t consumed() const { return pos_; }
  // after kError, no CR/LF so it can go straight to the client
  const std::string& error() const { return error_; }

  // call once the request is consumed
  void reset();

 private:
  enum class State { kStart, kInline, kCount, kBulkHeader, kBulkBody, kError };
  enum class Line { kNeedMore, kReady, kTooLong, kBadTerminator };
  struct Span {
    size_t offset;
    size_t length;
  };

  Status parse_inline(const char* data, size_t len);
  Line next_line(const char* data, size_t len, std::string_view* line);
  Status fail(std::string message);
  Status need_more(size_t len);
  Status finish(const char* data);
  bool split_inline(std::string_view line);

  State state_ = State::kStart;
  size_t pos_ = 0;   // next unparsed element
  size_t scan_ = 0;  // where the CRLF search resumes
  size_t remaining_ = 0;
  size_t bulk_len_ = 0;
  std::vector<Span> spans_;
  std::vector<std::string_view> args_;
  std::string inline_storage_;
  std::string error_;
};

// reply writers
void write_simple(net::Buffer& out, std::string_view s);
// s starts with the code ("ERR ..."), CR and LF become spaces
void write_error(net::Buffer& out, std::string_view s);
void write_integer(net::Buffer& out, int64_t v);
void write_bulk(net::Buffer& out, std::string_view s);
void write_null(net::Buffer& out);
void write_array_header(net::Buffer& out, size_t n);

}  // namespace proto
