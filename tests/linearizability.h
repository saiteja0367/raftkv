#pragma once

// Linearizability checker for key-value histories.
//
// A history is linearizable if every operation appears to take effect
// atomically at some instant between its invocation and its response, and the
// resulting sequential order is a legal execution of a single-copy key-value
// store. This is the standard correctness condition for a replicated store
// ("it behaves like one machine"), and the one Jepsen checks.
//
// Implementation: the Wing & Gong search with Lowe's memoization (as in
// Porcupine/Knossos). Linearizability is local (Herlihy & Wing), so each key's
// sub-history is checked independently, which keeps the search small.

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "kv/kv_store.h"

namespace raftkv::lin {

struct Operation {
  int client = 0;
  OpType type = OpType::Get;
  std::string key;
  std::string value;     // Put input
  Status status = Status::Ok;  // observed
  std::string output;    // Get output
  uint64_t call = 0;     // logical real-time of invocation
  uint64_t ret = 0;      // logical real-time of response (> call)
};

namespace detail {

// Register state for one key: absent, or a value.
using State = std::optional<std::string>;

// Sequential specification of one key.
inline bool Step(const State& s, const Operation& op, State& next) {
  switch (op.type) {
    case OpType::Put:
      next = op.value;
      return op.status == Status::Ok;
    case OpType::Get:
      next = s;
      if (!s) return op.status == Status::NotFound;
      return op.status == Status::Ok && op.output == *s;
    case OpType::Delete:
      next = std::nullopt;
      return op.status == (s ? Status::Ok : Status::NotFound);
  }
  return false;
}

struct Entry {
  bool is_call;
  size_t op;       // index into ops
  uint64_t time;
  Entry* prev = nullptr;
  Entry* next = nullptr;
  Entry* match = nullptr;  // call <-> return
};

struct CacheKey {
  std::vector<uint64_t> bits;
  State state;
  bool operator==(const CacheKey&) const = default;
};

struct CacheHash {
  size_t operator()(const CacheKey& k) const {
    size_t h = k.state ? std::hash<std::string>()(*k.state) : 0x9e3779b97f4a7c15ull;
    for (uint64_t w : k.bits) h ^= std::hash<uint64_t>()(w) + 0x9e3779b97f4a7c15ull + (h << 6) + (h >> 2);
    return h;
  }
};

// Checks one key's history. Returns true if linearizable.
inline bool CheckKey(const std::vector<Operation>& ops) {
  const size_t n = ops.size();
  if (n == 0) return true;

  // Build the event list sorted by time; calls before returns at equal times
  // are impossible because times come from one strictly increasing counter.
  std::vector<Entry> events;
  events.reserve(2 * n);
  for (size_t i = 0; i < n; ++i) {
    events.push_back({true, i, ops[i].call});
    events.push_back({false, i, ops[i].ret});
  }
  std::sort(events.begin(), events.end(), [](const Entry& a, const Entry& b) { return a.time < b.time; });
  std::vector<Entry*> call_of(n), ret_of(n);
  Entry head{true, SIZE_MAX, 0};
  Entry* prev = &head;
  for (auto& e : events) {
    e.prev = prev;
    prev->next = &e;
    prev = &e;
    (e.is_call ? call_of : ret_of)[e.op] = &e;
  }
  for (size_t i = 0; i < n; ++i) {
    call_of[i]->match = ret_of[i];
    ret_of[i]->match = call_of[i];
  }

  auto lift = [](Entry* call) {  // remove a call and its return from the list
    call->prev->next = call->next;
    if (call->next) call->next->prev = call->prev;
    Entry* r = call->match;
    r->prev->next = r->next;
    if (r->next) r->next->prev = r->prev;
  };
  auto unlift = [](Entry* call) {  // reinsert (reverse order of lift)
    Entry* r = call->match;
    r->prev->next = r;
    if (r->next) r->next->prev = r;
    call->prev->next = call;
    if (call->next) call->next->prev = call;
  };

  std::vector<uint64_t> linearized((n + 63) / 64, 0);
  std::unordered_set<CacheKey, CacheHash> cache;
  struct Frame {
    Entry* call;
    State state;
  };
  std::vector<Frame> stack;
  State state;  // keys start absent
  Entry* entry = head.next;

  while (head.next != nullptr) {
    if (entry->is_call) {
      State next = std::nullopt;
      if (Step(state, ops[entry->op], next)) {
        linearized[entry->op / 64] |= (uint64_t{1} << (entry->op % 64));
        CacheKey key;
        key.bits = linearized;
        key.state = next;
        if (cache.insert(key).second) {
          stack.push_back({entry, state});
          state = std::move(next);
          lift(entry);
          entry = head.next;
          continue;
        }
        linearized[entry->op / 64] &= ~(uint64_t{1} << (entry->op % 64));
      }
      entry = entry->next;
    } else {
      // Reached a return whose call can't be linearized yet: backtrack.
      if (stack.empty()) return false;
      Frame f = std::move(stack.back());
      stack.pop_back();
      state = std::move(f.state);
      linearized[f.call->op / 64] &= ~(uint64_t{1} << (f.call->op % 64));
      unlift(f.call);
      entry = f.call->next;
    }
  }
  return true;
}

}  // namespace detail

struct Result {
  bool ok = true;
  std::string bad_key;
  size_t operations = 0;
  size_t keys = 0;
};

inline Result Check(const std::vector<Operation>& history) {
  std::map<std::string, std::vector<Operation>> by_key;
  for (const auto& op : history) by_key[op.key].push_back(op);
  Result r;
  r.operations = history.size();
  r.keys = by_key.size();
  for (const auto& [key, ops] : by_key) {
    if (!detail::CheckKey(ops)) {
      r.ok = false;
      r.bad_key = key;
      return r;
    }
  }
  return r;
}

}  // namespace raftkv::lin
