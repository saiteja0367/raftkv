#pragma once

#include <cstdint>
#include <string_view>

namespace raftkv {

// Hash partitioning: every key belongs to exactly one shard, and each shard is
// replicated by its own independent consensus group. Linearizability is a
// local property (Herlihy & Wing), so per-key guarantees from each group
// compose into a linearizable store as a whole.
//
// FNV-1a is stable across platforms and builds, which matters because clients
// and servers must agree on the mapping.
inline uint64_t Fnv1a64(std::string_view s) {
  uint64_t h = 14695981039346656037ull;  // FNV-1a 64-bit offset basis
  for (unsigned char c : s) {
    h ^= c;
    h *= 1099511628211ull;
  }
  return h;
}

inline uint32_t ShardFor(std::string_view key, uint32_t num_shards) {
  return num_shards <= 1 ? 0 : static_cast<uint32_t>(Fnv1a64(key) % num_shards);
}

}  // namespace raftkv
