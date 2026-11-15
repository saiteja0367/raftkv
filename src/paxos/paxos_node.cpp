#include "paxos/paxos_node.h"

#include <algorithm>
#include <stdexcept>
#include <functional>

namespace raftkv {

PaxosNode::PaxosNode(NodeId id, std::vector<NodeId> peers, PaxosStorage& storage, uint64_t seed,
                     PaxosConfig config)
    : id_(id), peers_(std::move(peers)), storage_(storage), config_(config), rng_(seed) {
  max_round_seen_ = BallotRound(storage_.promised());
  // Every slot in the snapshot was chosen before it was taken.
  commit_ = stable_ = storage_.snapshot_index();
  ResetElectionTimer();
}

std::string PaxosNode::StateString() const {
  const char* mode = mode_ == Mode::Leading ? "leader" : mode_ == Mode::Preparing ? "preparing" : "follower";
  return std::string("ballot=") + std::to_string(BallotRound(epoch())) + "." +
         std::to_string(BallotNode(epoch())) + " role=" + mode + " leader=" + std::to_string(leader_) +
         " commit=" + std::to_string(commit_);
}

void PaxosNode::ResetElectionTimer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> dist(config_.election_timeout_min, config_.election_timeout_max - 1);
  election_timeout_ = dist(rng_);
}

void PaxosNode::Send(Message m) {
  m.from = id_;
  outbox_.push_back(std::move(m));
}

std::vector<Message> PaxosNode::TakeMessages() {
  std::vector<Message> out;
  out.swap(outbox_);
  return out;
}

std::optional<Snapshot> PaxosNode::TakeSnapshot() {
  if (applied_ >= storage_.snapshot_index()) return std::nullopt;
  applied_ = storage_.snapshot_index();
  return Snapshot{applied_, storage_.snapshot_data()};
}

void PaxosNode::Compact(Index index, std::string data) {
  if (index <= storage_.snapshot_index()) return;
  if (index > applied_) throw std::logic_error("cannot compact past the applied index");
  storage_.SetSnapshot(index, std::move(data));
}

std::vector<LogEntry> PaxosNode::TakeCommitted() {
  if (applied_ < storage_.snapshot_index()) {
    throw std::logic_error("TakeSnapshot() must be called before TakeCommitted()");
  }
  std::vector<LogEntry> out;
  const auto& acc = storage_.accepted();
  while (applied_ < commit_) {
    ++applied_;
    // Invariant: every slot <= commit_ holds its chosen value locally.
    out.push_back(acc.at(applied_));
  }
  return out;
}

// ---------------------------------------------------------------------------
// Leadership
// ---------------------------------------------------------------------------

void PaxosNode::Tick() {
  if (mode_ == Mode::Leading) {
    for (auto& [p, cd] : snapshot_cooldown_) {
      if (cd > 0) --cd;
    }
    if (++heartbeat_elapsed_ >= config_.heartbeat_interval) {
      heartbeat_elapsed_ = 0;
      BroadcastAccept();
    }
    return;
  }
  // Follower without a live leader, or a Preparing node whose round stalled.
  if (++election_elapsed_ >= election_timeout_) StartPrepare();
}

void PaxosNode::StartPrepare() {
  // A ballot strictly higher than anything promised or seen, unique to us.
  uint64_t round = std::max(max_round_seen_, BallotRound(storage_.promised())) + 1;
  ballot_ = MakeBallot(round, id_);
  max_round_seen_ = round;
  mode_ = Mode::Preparing;
  leader_ = kNoNode;
  prepare_from_ = commit_ + 1;
  promises_.clear();
  ResetElectionTimer();

  // We are our own first acceptor: promise durably, and contribute our own
  // accepted values to the recovery.
  storage_.SetPromised(ballot_);
  stable_ = commit_;
  std::vector<LogEntry>& mine = promises_[id_].entries;  // our snapshot <= commit_: never needed
  for (auto it = storage_.accepted().lower_bound(prepare_from_); it != storage_.accepted().end(); ++it) {
    mine.push_back(it->second);
  }
  if (promises_.size() >= Quorum()) {
    BecomeLeader();
    return;
  }
  for (NodeId p : peers_) {
    Message m;
    m.type = MsgType::Prepare;
    m.to = p;
    m.term = ballot_;
    m.prev_log_index = prepare_from_;
    Send(std::move(m));
  }
}

void PaxosNode::BecomeLeader() {
  // If some acceptor compacted slots we have not learned yet, those slots are
  // chosen but their individual values are gone from its promise. Treating
  // them as empty would let us fill chosen slots with no-ops. Instead, adopt
  // the newest such snapshot (it is a chosen prefix) and recover after it.
  const PromiseInfo* newest = nullptr;
  for (const auto& [node, info] : promises_) {
    if (info.snapshot_index >= prepare_from_ && (!newest || info.snapshot_index > newest->snapshot_index)) {
      newest = &info;
    }
  }
  if (newest) {
    storage_.SetSnapshot(newest->snapshot_index, newest->snapshot);
    commit_ = std::max(commit_, newest->snapshot_index);
    stable_ = commit_;
    prepare_from_ = commit_ + 1;
  }

  // Phase-1 value recovery. For each undecided slot, re-propose the value
  // accepted with the highest ballot by any promising acceptor. If a value
  // was (or may have been) chosen, every majority contains an acceptor that
  // accepted it, and no higher-ballot value can exist for that slot, so this
  // rule always preserves it. Empty slots ("holes") become no-ops.
  std::map<Index, LogEntry> best;
  Index max_slot = prepare_from_ - 1;
  for (const auto& [node, info] : promises_) {
    for (const auto& e : info.entries) {
      if (e.index < prepare_from_) continue;
      max_slot = std::max(max_slot, e.index);
      auto it = best.find(e.index);
      if (it == best.end() || e.term > it->second.term) best[e.index] = e;
    }
  }
  std::vector<LogEntry> recovered;
  for (Index s = prepare_from_; s <= max_slot; ++s) {
    auto it = best.find(s);
    recovered.push_back(LogEntry{ballot_, s, it == best.end() ? std::string() : it->second.data});
  }
  storage_.Accept(recovered);  // we accept our own proposals at our ballot

  mode_ = Mode::Leading;
  leader_ = id_;
  heartbeat_elapsed_ = 0;
  next_slot_ = max_slot + 1;
  promises_.clear();
  next_.clear();
  match_.clear();
  for (NodeId p : peers_) {
    next_[p] = prepare_from_;
    match_[p] = 0;
  }
  AdvanceCommit();
  BroadcastAccept();
}

void PaxosNode::StepDown(Ballot seen) {
  max_round_seen_ = std::max(max_round_seen_, BallotRound(seen));
  if (mode_ != Mode::Follower) {
    mode_ = Mode::Follower;
    leader_ = kNoNode;
    ResetElectionTimer();
  }
}

std::optional<Index> PaxosNode::ProposeBatch(std::vector<std::string> batch) {
  if (mode_ != Mode::Leading || batch.empty()) return std::nullopt;
  Index first = next_slot_;
  std::vector<LogEntry> entries;
  for (auto& cmd : batch) entries.push_back(LogEntry{ballot_, next_slot_++, std::move(cmd)});
  storage_.Accept(entries);  // one durable write for the batch
  AdvanceCommit();
  BroadcastAccept();
  return first;
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

void PaxosNode::Step(const Message& m) {
  switch (m.type) {
    case MsgType::Prepare: HandlePrepare(m); break;
    case MsgType::Promise: HandlePromise(m); break;
    case MsgType::Accept: HandleAccept(m); break;
    case MsgType::Accepted: HandleAccepted(m); break;
    case MsgType::InstallSnapshot: HandleInstallSnapshot(m); break;
    default: break;  // not a Paxos message
  }
}

void PaxosNode::HandlePrepare(const Message& m) {
  Message r;
  r.type = MsgType::Promise;
  r.to = m.from;
  if (m.term <= storage_.promised()) {
    // Already promised an equal or higher ballot: reject, telling the
    // proposer what to beat.
    r.success = false;
    r.term = storage_.promised();
    Send(std::move(r));
    return;
  }
  storage_.SetPromised(m.term);  // durable before promising
  StepDown(m.term);
  leader_ = kNoNode;
  stable_ = commit_;
  ResetElectionTimer();  // give the proposer time to finish

  r.success = true;
  r.term = m.term;
  r.match_index = commit_;
  for (auto it = storage_.accepted().lower_bound(m.prev_log_index); it != storage_.accepted().end(); ++it) {
    r.entries.push_back(it->second);
  }
  if (storage_.snapshot_index() >= m.prev_log_index) {
    // Slots up to our snapshot were compacted: report the snapshot instead.
    r.prev_log_index = storage_.snapshot_index();
    r.snapshot = storage_.snapshot_data();
  }
  Send(std::move(r));
}

void PaxosNode::HandlePromise(const Message& m) {
  if (!m.success) {
    if (m.term > ballot_) StepDown(m.term);
    return;
  }
  if (mode_ != Mode::Preparing || m.term != ballot_) return;  // stale
  promises_[m.from] = PromiseInfo{m.entries, m.prev_log_index, m.snapshot};
  if (promises_.size() >= Quorum()) BecomeLeader();
}

void PaxosNode::HandleAccept(const Message& m) {
  Message r;
  r.type = MsgType::Accepted;
  r.to = m.from;
  if (m.term < storage_.promised()) {
    r.success = false;
    r.term = storage_.promised();
    Send(std::move(r));
    return;
  }
  if (m.term > storage_.promised()) {
    // A leader with a newer ballot: adopt it. Our earlier acceptances were at
    // other ballots, so only already-chosen slots remain "stable".
    storage_.SetPromised(m.term);
    stable_ = commit_;
  }
  if (mode_ != Mode::Follower && m.term > ballot_) StepDown(m.term);
  leader_ = m.from;
  ResetElectionTimer();

  r.term = m.term;
  if (!m.entries.empty()) {
    Index first = m.prev_log_index;
    if (first > stable_ + 1) {
      // Gap: we are missing earlier slots from this leader. Ask it to back up.
      r.success = false;
      r.match_index = stable_;
      Send(std::move(r));
      return;
    }
    // Persist only what is new. The leader re-sends entries a follower may
    // already hold (pipelining, lost acks); re-writing those would turn every
    // retransmission into a disk write.
    const auto& acc = storage_.accepted();
    std::vector<LogEntry> fresh;
    for (const auto& e : m.entries) {
      auto it = acc.find(e.index);
      if (it != acc.end() && it->second.term == m.term && it->second.data == e.data) continue;
      fresh.push_back(LogEntry{m.term, e.index, e.data});
    }
    storage_.Accept(fresh);  // durable before acknowledging
    stable_ = std::max(stable_, m.entries.back().index);
  }
  // Slots up to stable_ hold this leader's values (or already-chosen ones),
  // so whatever the leader reports as chosen up to there is chosen here too.
  commit_ = std::max(commit_, std::min(m.leader_commit, stable_));

  r.success = true;
  r.match_index = stable_;
  Send(std::move(r));
}

void PaxosNode::HandleAccepted(const Message& m) {
  if (m.term > ballot_ && !m.success) {
    StepDown(m.term);  // someone promised a higher ballot: we are deposed
    return;
  }
  if (mode_ != Mode::Leading || m.term != ballot_) return;  // stale
  if (m.success) {
    match_[m.from] = std::max(match_[m.from], m.match_index);
    next_[m.from] = match_[m.from] + 1;
    AdvanceCommit();
    if (next_[m.from] < next_slot_) SendAccept(m.from);
  } else {
    // Gap reported by the follower: resend from where it is.
    next_[m.from] = std::max<Index>(1, m.match_index + 1);
    SendAccept(m.from);
  }
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

void PaxosNode::SendAccept(NodeId peer) {
  Index from = std::max<Index>(1, next_[peer]);
  if (from <= storage_.snapshot_index()) {
    // The follower needs slots we compacted: send the snapshot instead
    // (rate-limited, since snapshots can be large).
    int& cooldown = snapshot_cooldown_[peer];
    if (cooldown > 0) return;
    cooldown = config_.election_timeout_min;
    Message m;
    m.type = MsgType::InstallSnapshot;
    m.to = peer;
    m.term = ballot_;
    m.prev_log_index = storage_.snapshot_index();
    m.leader_commit = commit_;
    m.snapshot = storage_.snapshot_data();
    Send(std::move(m));
    return;
  }
  Message m;
  m.type = MsgType::Accept;
  m.to = peer;
  m.term = ballot_;
  m.prev_log_index = from;
  m.leader_commit = commit_;
  const auto& acc = storage_.accepted();
  Index last = std::min<Index>(next_slot_ - 1, from + config_.max_entries_per_accept - 1);
  for (Index s = from; s <= last; ++s) {
    // Slots below our recovery point are chosen; resending them under our
    // ballot is safe because any proposal for a chosen slot carries its value.
    LogEntry e = acc.at(s);
    e.term = ballot_;
    m.entries.push_back(std::move(e));
  }
  Send(std::move(m));
}

void PaxosNode::BroadcastAccept() {
  for (NodeId p : peers_) SendAccept(p);
}

void PaxosNode::HandleInstallSnapshot(const Message& m) {
  Message r;
  r.type = MsgType::Accepted;
  r.to = m.from;
  if (m.term < storage_.promised()) {
    r.success = false;
    r.term = storage_.promised();
    Send(std::move(r));
    return;
  }
  if (m.term > storage_.promised()) {
    storage_.SetPromised(m.term);
    stable_ = commit_;
  }
  if (mode_ != Mode::Follower && m.term > ballot_) StepDown(m.term);
  leader_ = m.from;
  ResetElectionTimer();

  if (m.prev_log_index > commit_) {
    // Every slot up to the snapshot is chosen; the application restores its
    // state machine from it via TakeSnapshot().
    storage_.SetSnapshot(m.prev_log_index, m.snapshot);
    commit_ = m.prev_log_index;
    stable_ = std::max(stable_, commit_);
  }
  r.success = true;
  r.term = m.term;
  r.match_index = stable_;
  Send(std::move(r));
}

void PaxosNode::AdvanceCommit() {
  // A slot is chosen once a majority has accepted it at our ballot (or knows
  // it was already chosen). match_ values are contiguous, so the quorum-th
  // highest match is the highest slot for which that holds.
  std::vector<Index> matches;
  matches.push_back(next_slot_ - 1);  // ourselves
  for (const auto& [p, idx] : match_) matches.push_back(idx);
  std::sort(matches.begin(), matches.end(), std::greater<Index>());
  Index candidate = matches[Quorum() - 1];
  if (candidate > commit_) commit_ = candidate;
}

}  // namespace raftkv
