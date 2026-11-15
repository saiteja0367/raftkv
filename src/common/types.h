#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace raftkv {

using NodeId = uint32_t;
using Term = uint64_t;
using Index = uint64_t;

// Node ids start at 1; 0 means "nobody" (no vote cast, leader unknown).
constexpr NodeId kNoNode = 0;

// One entry in the replicated log. An empty `data` is a leader no-op entry,
// appended on election so entries from earlier terms can be committed.
struct LogEntry {
  Term term = 0;
  Index index = 0;
  std::string data;

  bool operator==(const LogEntry&) const = default;
};

enum class MsgType : uint8_t {
  // Raft
  RequestVote = 1,
  RequestVoteResp = 2,
  AppendEntries = 3,
  AppendEntriesResp = 4,
  // Multi-Paxos
  Prepare = 5,   // phase 1a
  Promise = 6,   // phase 1b (success=false: rejected, term = higher promised ballot)
  Accept = 7,    // phase 2a (no entries = heartbeat carrying the commit index)
  Accepted = 8,  // phase 2b (success=false: rejected, term = higher promised ballot)
  // Both protocols
  InstallSnapshot = 9,  // leader -> lagging follower: state up to prev_log_index
  // Raft pre-vote (section 9.6 of Ongaro's thesis)
  PreVote = 10,      // "would you vote for me in term+1?" (changes no state)
  PreVoteResp = 11,
};
constexpr uint8_t kMaxMsgType = 11;

// A single consensus RPC message. One struct covers every message type of both
// protocols; fields a type does not use are left at their defaults.
//
// Field reuse for Multi-Paxos:
//   term           = ballot
//   prev_log_index = first slot (Prepare: recover from here; Accept: first entry)
//   entries        = (slot, accepted ballot, value) triples as LogEntry{term, index, data}
//   leader_commit  = leader's commit index (highest contiguous chosen slot)
//   match_index    = acceptor's contiguous slot (Accepted) / commit index (Promise)
struct Message {
  MsgType type = MsgType::RequestVote;
  uint32_t group = 0;  // which consensus group (shard) this message belongs to
  NodeId from = kNoNode;
  NodeId to = kNoNode;
  Term term = 0;

  // RequestVote/PreVote: candidate's last log position (up-to-date check).
  // AppendEntries/Resp: reused as the ReadIndex heartbeat round (echoed back).
  Index last_log_index = 0;
  Term last_log_term = 0;

  // AppendEntries.
  Index prev_log_index = 0;
  Term prev_log_term = 0;
  std::vector<LogEntry> entries;
  Index leader_commit = 0;

  // Responses. `success` = vote granted / append accepted.
  // `match_index` = on success, the follower's last replicated index;
  //                 on append failure, a hint for where the leader should back up to.
  bool success = false;
  Index match_index = 0;

  // InstallSnapshot (and Paxos Promise): serialized state machine covering
  // every log index up to prev_log_index (whose term/ballot is prev_log_term).
  std::string snapshot;

  bool operator==(const Message&) const = default;
};

}  // namespace raftkv
