#pragma once

#include <charconv>
#include <cstdint>
#include <string_view>

namespace engine {

// strict int64 like redis: no '+', spaces, leading zeros or "-0"
inline bool parse_int64(std::string_view s, int64_t* out) {
  if (s.empty() || s.size() > 20) return false;
  const bool negative = s[0] == '-';
  const std::string_view digits = s.substr(negative ? 1 : 0);
  if (digits.empty()) return false;
  if (digits[0] == '0' && (digits.size() > 1 || negative)) return false;
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(s.data(), end, *out);
  return ec == std::errc() && ptr == end;
}

}  // namespace engine
