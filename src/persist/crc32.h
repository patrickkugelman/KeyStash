#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace persist {

namespace detail {
constexpr std::array<uint32_t, 256> make_crc_table() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
    table[i] = c;
  }
  return table;
}
inline constexpr std::array<uint32_t, 256> kCrcTable = make_crc_table();
}  // namespace detail

// IEEE CRC-32. Pass the previous result as `crc` to checksum in pieces.
inline uint32_t crc32(const void* data, size_t n, uint32_t crc = 0) {
  const auto* p = static_cast<const unsigned char*>(data);
  uint32_t c = ~crc;
  for (size_t i = 0; i < n; ++i) c = detail::kCrcTable[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
  return ~c;
}

}  // namespace persist
