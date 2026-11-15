#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

namespace raftkv {

enum class OpType : uint8_t { Put = 1, Get = 2, Delete = 3 };
enum class Status : uint8_t { Ok = 0, NotFound = 1, NotLeader = 2, Retry = 3, Error = 4 };

const char* StatusName(Status s);

// A client command as stored in the Raft log. (client_id, seq) identifies the
// request so retries after a timeout or leader change are applied once.
struct Command {
  uint64_t client_id = 0;
  uint64_t seq = 0;
  OpType op = OpType::Get;
  std::string key;
  std::string value;

  bool operator==(const Command&) const = default;
};

std::string EncodeCommand(const Command& c);
Command DecodeCommand(std::string_view data);  // throws DecodeError

struct ApplyResult {
  Status status = Status::Ok;
  std::string value;

  bool operator==(const ApplyResult&) const = default;
};

// The replicated state machine: an in-memory key-value map built by applying
// committed log entries in order. Every replica applies the same entries in
// the same order, so every replica ends in the same state.
//
// Exactly-once semantics: a client may resend a request whose response was
// lost (timeout, leader crash). The store remembers the last (seq, result)
// per client and returns the cached result for a duplicate instead of
// applying it again, so a retried PUT/DELETE never takes effect twice.
class KvStore {
 public:
  ApplyResult Apply(const Command& c);

  // Snapshot of the full state machine, including client sessions: the
  // dedup table must survive log compaction, or a retry that arrives after a
  // snapshot could be applied twice.
  std::string Serialize() const;
  void Restore(std::string_view snapshot);  // throws DecodeError

  size_t size() const { return data_.size(); }
  const std::unordered_map<std::string, std::string>& data() const { return data_; }

 private:
  struct Session {
    uint64_t last_seq = 0;
    ApplyResult last_result;
  };

  std::unordered_map<std::string, std::string> data_;
  std::unordered_map<uint64_t, Session> sessions_;
};

}  // namespace raftkv
