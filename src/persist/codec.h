#pragma once

#include <cstdint>
#include <string>

namespace persist {

// Fixed little-endian integers, independent of host byte order.
inline void store_u32(char* p, uint32_t v) {
  for (int i = 0; i < 4; ++i) p[i] = static_cast<char>((v >> (8 * i)) & 0xFFu);
}

inline void put_u32(std::string& s, uint32_t v) {
  char b[4];
  store_u32(b, v);
  s.append(b, 4);
}

inline void put_u64(std::string& s, uint64_t v) {
  char b[8];
  for (int i = 0; i < 8; ++i) b[i] = static_cast<char>((v >> (8 * i)) & 0xFFu);
  s.append(b, 8);
}

inline uint32_t get_u32(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  uint32_t v = 0;
  for (int i = 3; i >= 0; --i) v = (v << 8) | u[i];
  return v;
}

inline uint64_t get_u64(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  uint64_t v = 0;
  for (int i = 7; i >= 0; --i) v = (v << 8) | u[i];
  return v;
}

}  // namespace persist
