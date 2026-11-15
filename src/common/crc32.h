#pragma once

#include <cstddef>
#include <cstdint>

namespace raftkv {

// Standard CRC-32 (IEEE 802.3 polynomial), used to detect torn or corrupted
// records in the write-ahead log.
uint32_t crc32(const void* data, size_t len, uint32_t seed = 0);

}  // namespace raftkv
