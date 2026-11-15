#include "common/crc32.h"

#include <array>

namespace raftkv {
namespace {

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) {
      c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
    }
    table[i] = c;
  }
  return table;
}

constexpr auto kTable = MakeTable();

}  // namespace

uint32_t crc32(const void* data, size_t len, uint32_t seed) {
  const auto* p = static_cast<const uint8_t*>(data);
  uint32_t c = seed ^ 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    c = kTable[(c ^ p[i]) & 0xFF] ^ (c >> 8);
  }
  return c ^ 0xFFFFFFFFu;
}

}  // namespace raftkv
