#include "proto/resp.h"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <utility>

namespace proto {
namespace {

bool parse_i64(std::string_view s, int64_t* out) {
  if (s.empty()) return false;
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(s.data(), end, *out);
  return ec == std::errc() && ptr == end;
}

bool is_space(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

int hex_value(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

char unescape(char c) {
  switch (c) {
    case 'n': return '\n';
    case 'r': return '\r';
    case 't': return '\t';
    case 'b': return '\b';
    case 'a': return '\a';
    default: return c;
  }
}

}  // namespace

Parser::Status Parser::fail(std::string message) {
  state_ = State::kError;
  error_ = std::move(message);
  return Status::kError;
}

// `len` starts at the current request, so this caps one request's total size.
Parser::Status Parser::need_more(size_t len) {
  if (len > kMaxRequest) return fail("too big request");
  return Status::kNeedMore;
}

// Finds the CRLF ending the line at pos_ and advances pos_ past it.
Parser::Line Parser::next_line(const char* data, size_t len, std::string_view* line) {
  const size_t from = std::max(pos_, scan_);
  const void* hit = from < len ? std::memchr(data + from, '\r', len - from) : nullptr;
  if (hit == nullptr) {
    scan_ = len;
    return len - pos_ > kMaxLine ? Line::kTooLong : Line::kNeedMore;
  }
  const size_t cr = static_cast<size_t>(static_cast<const char*>(hit) - data);
  if (cr - pos_ > kMaxLine) return Line::kTooLong;
  if (cr + 1 == len) {
    scan_ = cr;  // recheck this CR once its successor arrives
    return Line::kNeedMore;
  }
  if (data[cr + 1] != '\n') return Line::kBadTerminator;
  *line = std::string_view(data + pos_, cr - pos_);
  pos_ = cr + 2;
  scan_ = pos_;
  return Line::kReady;
}

// Redis-style splitting: whitespace, "double" with escapes, 'single' quotes.
bool Parser::split_inline(std::string_view line) {
  // Unescaped text never outgrows the line, so views stay stable.
  inline_storage_.clear();
  inline_storage_.reserve(line.size());
  size_t i = 0;
  const size_t n = line.size();
  while (true) {
    while (i < n && is_space(line[i])) ++i;
    if (i == n) return true;
    const size_t start = inline_storage_.size();
    bool in_double = false;
    bool in_single = false;
    bool done = false;
    while (!done) {
      if (in_double) {
        if (i == n) return false;
        const char c = line[i];
        if (c == '\\' && i + 3 < n && line[i + 1] == 'x' && hex_value(line[i + 2]) >= 0 &&
            hex_value(line[i + 3]) >= 0) {
          inline_storage_.push_back(
              static_cast<char>(hex_value(line[i + 2]) * 16 + hex_value(line[i + 3])));
          i += 4;
        } else if (c == '\\' && i + 1 < n) {
          inline_storage_.push_back(unescape(line[i + 1]));
          i += 2;
        } else if (c == '"') {
          ++i;
          if (i < n && !is_space(line[i])) return false;
          done = true;
        } else {
          inline_storage_.push_back(c);
          ++i;
        }
      } else if (in_single) {
        if (i == n) return false;
        const char c = line[i];
        if (c == '\\' && i + 1 < n && line[i + 1] == '\'') {
          inline_storage_.push_back('\'');
          i += 2;
        } else if (c == '\'') {
          ++i;
          if (i < n && !is_space(line[i])) return false;
          done = true;
        } else {
          inline_storage_.push_back(c);
          ++i;
        }
      } else {
        if (i == n || is_space(line[i])) {
          done = true;
        } else if (line[i] == '"') {
          in_double = true;
          ++i;
        } else if (line[i] == '\'') {
          in_single = true;
          ++i;
        } else {
          inline_storage_.push_back(line[i]);
          ++i;
        }
      }
    }
    args_.emplace_back(inline_storage_.data() + start, inline_storage_.size() - start);
  }
}

Parser::Status Parser::parse_inline(const char* data, size_t len) {
  const size_t from = std::max(pos_, scan_);
  const void* hit = from < len ? std::memchr(data + from, '\n', len - from) : nullptr;
  if (hit == nullptr) {
    scan_ = len;
    if (len > kMaxLine) return fail("too big inline request");
    return Status::kNeedMore;
  }
  const size_t nl = static_cast<size_t>(static_cast<const char*>(hit) - data);
  if (nl > kMaxLine) return fail("too big inline request");
  size_t end = nl;
  if (end > 0 && data[end - 1] == '\r') --end;
  if (!split_inline(std::string_view(data, end))) return fail("unbalanced quotes in request");
  pos_ = nl + 1;
  return Status::kComplete;
}

Parser::Status Parser::finish(const char* data) {
  args_.reserve(spans_.size());
  for (const Span& s : spans_) args_.emplace_back(data + s.offset, s.length);
  return Status::kComplete;
}

Parser::Status Parser::parse(const char* data, size_t len) {
  while (true) {
    switch (state_) {
      case State::kError:
        return Status::kError;

      case State::kStart:
        if (len == 0) return Status::kNeedMore;
        state_ = data[0] == '*' ? State::kCount : State::kInline;
        break;

      case State::kInline:
        return parse_inline(data, len);

      case State::kCount: {
        std::string_view line;
        const Line r = next_line(data, len, &line);
        if (r == Line::kNeedMore) return need_more(len);
        if (r == Line::kTooLong) return fail("too big mbulk count string");
        if (r == Line::kBadTerminator) return fail("invalid multibulk length");
        int64_t count = 0;
        if (!parse_i64(line.substr(1), &count) || count > static_cast<int64_t>(kMaxArgs)) {
          return fail("invalid multibulk length");
        }
        if (count <= 0) return Status::kComplete;
        remaining_ = static_cast<size_t>(count);
        // The count is client-controlled, so do not reserve from it.
        spans_.reserve(std::min<size_t>(remaining_, 64));
        state_ = State::kBulkHeader;
        break;
      }

      case State::kBulkHeader: {
        if (pos_ == len) return need_more(len);
        if (data[pos_] != '$') {
          const char c = data[pos_];
          const bool printable = c >= 0x20 && c < 0x7f;
          return fail(std::string("expected '$', got '") + (printable ? c : '?') + "'");
        }
        std::string_view line;
        const Line r = next_line(data, len, &line);
        if (r == Line::kNeedMore) return need_more(len);
        if (r == Line::kTooLong) return fail("too big bulk count string");
        if (r == Line::kBadTerminator) return fail("invalid bulk length");
        int64_t length = 0;
        if (!parse_i64(line.substr(1), &length) || length < 0 ||
            length > static_cast<int64_t>(kMaxBulk)) {
          return fail("invalid bulk length");
        }
        bulk_len_ = static_cast<size_t>(length);
        state_ = State::kBulkBody;
        break;
      }

      case State::kBulkBody: {
        if (len - pos_ < bulk_len_ + 2) return need_more(len);
        const char* tail = data + pos_ + bulk_len_;
        if (tail[0] != '\r' || tail[1] != '\n') return fail("expected CRLF after bulk payload");
        spans_.push_back({pos_, bulk_len_});
        pos_ += bulk_len_ + 2;
        scan_ = pos_;
        if (--remaining_ == 0) return finish(data);
        state_ = State::kBulkHeader;
        break;
      }
    }
  }
}

void Parser::reset() {
  state_ = State::kStart;
  pos_ = 0;
  scan_ = 0;
  remaining_ = 0;
  bulk_len_ = 0;
  // Drop outliers so one huge request does not pin memory per connection.
  constexpr size_t kRetainArgs = 1024;
  if (spans_.capacity() > kRetainArgs) {
    std::vector<Span>().swap(spans_);
    std::vector<std::string_view>().swap(args_);
  } else {
    spans_.clear();
    args_.clear();
  }
  if (inline_storage_.capacity() > kMaxLine) std::string().swap(inline_storage_);
}

void write_simple(net::Buffer& out, std::string_view s) {
  out.append("+", 1);
  out.append(s);
  out.append("\r\n", 2);
}

void write_error(net::Buffer& out, std::string_view s) {
  out.append("-", 1);
  for (char c : s) {
    const char safe = (c == '\r' || c == '\n') ? ' ' : c;
    out.append(&safe, 1);
  }
  out.append("\r\n", 2);
}

namespace {

void write_prefixed(net::Buffer& out, char prefix, int64_t v) {
  char buf[24];
  buf[0] = prefix;
  auto [ptr, ec] = std::to_chars(buf + 1, buf + sizeof(buf) - 2, v);
  (void)ec;  // 21 bytes always fit an int64_t
  ptr[0] = '\r';
  ptr[1] = '\n';
  out.append(buf, static_cast<size_t>(ptr + 2 - buf));
}

}  // namespace

void write_integer(net::Buffer& out, int64_t v) { write_prefixed(out, ':', v); }

void write_bulk(net::Buffer& out, std::string_view s) {
  write_prefixed(out, '$', static_cast<int64_t>(s.size()));
  out.append(s);
  out.append("\r\n", 2);
}

void write_null(net::Buffer& out) { out.append("$-1\r\n", 5); }

void write_array_header(net::Buffer& out, size_t n) {
  write_prefixed(out, '*', static_cast<int64_t>(n));
}

}  // namespace proto
