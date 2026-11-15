#include "raft/raft_node.h"

#include <algorithm>
#include <functional>
#include <stdexcept>

namespace raftkv {

const char* RoleName(Role r) {
  switch (r) {
    case Role::Follower: return "follower";
    case Role::PreCandidate: return "pre-candidate";
    case Role::Candidate: return "candidate";
    case Role::Leader: return "leader";
  }
  return "unknown";
}

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, Storage& storage, uint64_t seed,
                   RaftConfig config)
    : id_(id), peers_(std::move(peers)), storage_(storage), config_(config), rng_(seed) {
  // Everything in the snapshot was committed before it was taken.
  commit_ = storage_.snapshot_index();
  ResetElectionTimer();
}

std::string RaftNode::StateString() const {
  return std::string("term=") + std::to_string(term()) + " role=" + RoleName(role_) +
         " leader=" + std::to_string(leader_) + " commit=" + std::to_string(commit_) +
         " last=" + std::to_string(last_index());
}

Term RaftNode::TermAt(Index i) const {
  if (i == 0 || i > storage_.last_index()) return 0;
  if (i == storage_.snapshot_index()) return storage_.snapshot_term();
  if (i < storage_.snapshot_index()) return 0;  // compacted (only ever asked about committed indices)
  return storage_.At(i).term;
}

void RaftNode::ResetElectionTimer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> dist(config_.election_timeout_min,
                                          config_.election_timeout_max - 1);
  // Randomized timeouts make split votes unlikely to repeat.
  election_timeout_ = dist(rng_);
}

void RaftNode::Send(Message m) {
  m.from = id_;
  m.term = term();
  outbox_.push_back(std::move(m));
}

std::vector<Message> RaftNode::TakeMessages() {
  std::vector<Message> out;
  out.swap(outbox_);
  return out;
}

std::optional<Snapshot> RaftNode::TakeSnapshot() {
  if (applied_ >= storage_.snapshot_index()) return std::nullopt;
  applied_ = storage_.snapshot_index();
  return Snapshot{applied_, storage_.snapshot_data()};
}

std::vector<LogEntry> RaftNode::TakeCommitted() {
  if (applied_ < storage_.snapshot_index()) {
    throw std::logic_error("TakeSnapshot() must be called before TakeCommitted()");
  }
  std::vector<LogEntry> out;
  while (applied_ < commit_) {
    ++applied_;
    out.push_back(storage_.At(applied_));
  }
  return out;
}

void RaftNode::Compact(Index index, std::string data) {
  if (index <= storage_.snapshot_index()) return;
  if (index > applied_) throw std::logic_error("cannot compact past the applied index");
  storage_.Compact(index, TermAt(index), std::move(data));
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

void RaftNode::ClearLeaderState() {
  // Reads registered as leader can no longer be confirmed by us; the
  // application fails them and the client retries elsewhere.
  reads_awaiting_term_commit_.clear();
  reads_pending_.clear();
  peer_round_.clear();
  active_.clear();
}

void RaftNode::BecomeFollower(Term term, NodeId leader) {
  if (term > this->term()) {
    // New term: forget any vote from the old one. Persisted before we act.
    storage_.SetHardState({term, kNoNode});
  }
  if (role_ == Role::Leader) ClearLeaderState();
  role_ = Role::Follower;
  leader_ = leader;
  votes_.clear();
  prevotes_.clear();
  ResetElectionTimer();
}

void RaftNode::BecomePreCandidate() {
  // Ask "would you vote for me in term+1?" without changing our term, so a
  // node that cannot win (e.g. it was partitioned away) never disrupts the
  // cluster's current term.
  role_ = Role::PreCandidate;
  leader_ = kNoNode;
  prevotes_ = {id_};
  ResetElectionTimer();
  if (prevotes_.size() >= Quorum()) {
    BecomeCandidate();
    return;
  }
  for (NodeId p : peers_) {
    Message m;
    m.type = MsgType::PreVote;
    m.from = id_;
    m.to = p;
    m.term = term() + 1;  // the term we would campaign in (not persisted)
    m.last_log_index = last_index();
    m.last_log_term = TermAt(last_index());
    outbox_.push_back(std::move(m));  // bypass Send(): it stamps our current term
  }
}

void RaftNode::BecomeCandidate() {
  // Vote for ourselves in a fresh term; durable before requesting votes.
  storage_.SetHardState({term() + 1, id_});
  role_ = Role::Candidate;
  leader_ = kNoNode;
  votes_ = {id_};
  prevotes_.clear();
  ResetElectionTimer();

  if (votes_.size() >= Quorum()) {  // single-node cluster
    BecomeLeader();
    return;
  }
  for (NodeId p : peers_) {
    Message m;
    m.type = MsgType::RequestVote;
    m.to = p;
    m.last_log_index = last_index();
    m.last_log_term = TermAt(last_index());
    Send(std::move(m));
  }
}

void RaftNode::BecomeLeader() {
  role_ = Role::Leader;
  leader_ = id_;
  heartbeat_elapsed_ = 0;
  ClearLeaderState();
  quorum_check_elapsed_ = 0;
  next_index_.clear();
  match_index_.clear();
  for (NodeId p : peers_) {
    next_index_[p] = last_index() + 1;
    match_index_[p] = 0;
  }
  // A leader may only count replicas for entries of its own term (Raft
  // paper, section 5.4.2). Appending a no-op lets earlier-term entries
  // become committed promptly instead of waiting for the next client write.
  storage_.Append({LogEntry{term(), last_index() + 1, ""}});
  MaybeAdvanceCommit();
  BroadcastAppend();
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

void RaftNode::Tick() {
  if (role_ == Role::Leader) {
    for (auto& [p, cd] : snapshot_cooldown_) {
      if (cd > 0) --cd;
    }
    if (config_.check_quorum && ++quorum_check_elapsed_ >= config_.election_timeout_min) {
      quorum_check_elapsed_ = 0;
      if (active_.size() + 1 < Quorum()) {
        // We can't reach a majority: a new leader may already exist on the
        // other side. Stop accepting work instead of serving as a stale leader.
        BecomeFollower(term(), kNoNode);
        return;
      }
      active_.clear();
    }
    if (++heartbeat_elapsed_ >= config_.heartbeat_interval) {
      heartbeat_elapsed_ = 0;
      BroadcastAppend();
    }
    return;
  }
  if (++election_elapsed_ >= election_timeout_) {
    if (config_.pre_vote) {
      BecomePreCandidate();
    } else {
      BecomeCandidate();
    }
  }
}

std::optional<Index> RaftNode::ProposeBatch(std::vector<std::string> batch) {
  if (role_ != Role::Leader || batch.empty()) return std::nullopt;
  Index first = last_index() + 1;
  std::vector<LogEntry> entries;
  entries.reserve(batch.size());
  for (size_t i = 0; i < batch.size(); ++i) {
    entries.push_back(LogEntry{term(), first + i, std::move(batch[i])});
  }
  storage_.Append(entries);  // one durable write for the whole batch
  MaybeAdvanceCommit();      // single-node cluster commits immediately
  BroadcastAppend();
  return first;
}

void RaftNode::Step(const Message& m) {
  // Pre-vote messages carry a hypothetical term and must not change ours.
  if (m.type == MsgType::PreVote) {
    HandlePreVote(m);
    return;
  }
  if (m.type == MsgType::PreVoteResp) {
    HandlePreVoteResp(m);
    return;
  }
  switch (m.type) {
    case MsgType::RequestVote:
    case MsgType::RequestVoteResp:
    case MsgType::AppendEntries:
    case MsgType::AppendEntriesResp:
    case MsgType::InstallSnapshot:
      break;
    default:
      return;  // not a Raft message (e.g. a misconfigured Paxos peer)
  }
  if (m.term > term()) {
    // Any message from a newer term demotes us. If it is an AppendEntries,
    // its sender is the legitimate leader of that term.
    bool from_leader = m.type == MsgType::AppendEntries || m.type == MsgType::InstallSnapshot;
    BecomeFollower(m.term, from_leader ? m.from : kNoNode);
  } else if (m.term < term()) {
    // Stale sender. Reply to requests so it learns the newer term and steps
    // down; drop stale responses.
    if (m.type == MsgType::RequestVote) {
      Message r;
      r.type = MsgType::RequestVoteResp;
      r.to = m.from;
      r.success = false;
      Send(std::move(r));
    } else if (m.type == MsgType::AppendEntries || m.type == MsgType::InstallSnapshot) {
      Message r;
      r.type = MsgType::AppendEntriesResp;
      r.to = m.from;
      r.success = false;
      Send(std::move(r));
    }
    return;
  }

  switch (m.type) {
    case MsgType::RequestVote: HandleRequestVote(m); break;
    case MsgType::RequestVoteResp: HandleRequestVoteResp(m); break;
    case MsgType::AppendEntries: HandleAppendEntries(m); break;
    case MsgType::AppendEntriesResp: HandleAppendEntriesResp(m); break;
    case MsgType::InstallSnapshot: HandleInstallSnapshot(m); break;
    default: break;
  }
}

// ---------------------------------------------------------------------------
// Elections
// ---------------------------------------------------------------------------

void RaftNode::HandleRequestVote(const Message& m) {
  HardState hs = storage_.hard_state();
  bool can_vote = hs.voted_for == kNoNode || hs.voted_for == m.from;

  // Election restriction (section 5.4.1): only vote for a candidate whose
  // log is at least as up to date as ours, so a new leader always holds
  // every committed entry.
  Term my_last_term = TermAt(last_index());
  bool up_to_date = m.last_log_term > my_last_term ||
                    (m.last_log_term == my_last_term && m.last_log_index >= last_index());

  Message r;
  r.type = MsgType::RequestVoteResp;
  r.to = m.from;
  r.success = can_vote && up_to_date;
  if (r.success) {
    storage_.SetHardState({hs.term, m.from});  // durable before replying
    ResetElectionTimer();
  }
  Send(std::move(r));
}

void RaftNode::HandlePreVote(const Message& m) {
  // Grant only if (1) the candidate's term would be newer, (2) its log is at
  // least as up to date as ours, and (3) we have not heard from a live leader
  // recently ("leader stickiness"). No state changes either way.
  Term my_last_term = TermAt(last_index());
  bool up_to_date = m.last_log_term > my_last_term ||
                    (m.last_log_term == my_last_term && m.last_log_index >= last_index());
  bool leader_alive = role_ == Role::Leader ||
                      (leader_ != kNoNode && election_elapsed_ < config_.election_timeout_min);
  Message r;
  r.type = MsgType::PreVoteResp;
  r.from = id_;
  r.to = m.from;
  r.success = m.term > term() && up_to_date && !leader_alive;
  r.term = r.success ? m.term : term();
  outbox_.push_back(std::move(r));
}

void RaftNode::HandlePreVoteResp(const Message& m) {
  if (!m.success) {
    if (m.term > term()) BecomeFollower(m.term, kNoNode);  // we are behind
    return;
  }
  if (role_ != Role::PreCandidate || m.term != term() + 1) return;  // stale
  prevotes_.insert(m.from);
  if (prevotes_.size() >= Quorum()) BecomeCandidate();
}

void RaftNode::HandleRequestVoteResp(const Message& m) {
  if (role_ != Role::Candidate || !m.success) return;
  votes_.insert(m.from);
  if (votes_.size() >= Quorum()) BecomeLeader();
}

// ---------------------------------------------------------------------------
// Log replication
// ---------------------------------------------------------------------------

void RaftNode::SendAppend(NodeId peer) {
  Index next = next_index_[peer];
  if (next <= storage_.snapshot_index()) {
    // The entries this follower needs were compacted: send the snapshot.
    // Snapshots can be large, so don't resend one on every heartbeat.
    int& cooldown = snapshot_cooldown_[peer];
    if (cooldown > 0) return;
    cooldown = config_.election_timeout_min;
    Message m;
    m.type = MsgType::InstallSnapshot;
    m.to = peer;
    m.prev_log_index = storage_.snapshot_index();
    m.prev_log_term = storage_.snapshot_term();
    m.leader_commit = commit_;
    m.snapshot = storage_.snapshot_data();
    Send(std::move(m));
    return;
  }
  Message m;
  m.type = MsgType::AppendEntries;
  m.to = peer;
  m.prev_log_index = next - 1;
  m.prev_log_term = TermAt(next - 1);
  m.leader_commit = commit_;
  m.last_log_index = read_round_;  // ReadIndex: followers echo the round back
  Index last = std::min(last_index(), next + config_.max_entries_per_append - 1);
  for (Index i = next; i <= last; ++i) m.entries.push_back(storage_.At(i));
  Send(std::move(m));
}

void RaftNode::BroadcastAppend() {
  for (NodeId p : peers_) SendAppend(p);
}

void RaftNode::HandleAppendEntries(const Message& m) {
  // Same term as ours: the sender is this term's leader.
  if (role_ != Role::Follower) BecomeFollower(m.term, m.from);
  leader_ = m.from;
  ResetElectionTimer();

  Message r;
  r.type = MsgType::AppendEntriesResp;
  r.to = m.from;
  r.last_log_index = m.last_log_index;  // echo the ReadIndex round

  // Entries at or below our snapshot are committed and already match the
  // leader's: skip them and treat the snapshot point as the match point.
  const Message* msg = &m;
  Message trimmed;
  if (m.prev_log_index < storage_.snapshot_index()) {
    trimmed = m;
    Index snap = storage_.snapshot_index();
    trimmed.entries.clear();
    for (const auto& e : m.entries) {
      if (e.index > snap) trimmed.entries.push_back(e);
    }
    trimmed.prev_log_index = snap;
    trimmed.prev_log_term = storage_.snapshot_term();
    msg = &trimmed;
  }
  const Message& a = *msg;

  // Consistency check: we must hold the entry that precedes the new ones.
  if (a.prev_log_index > last_index()) {
    r.success = false;
    r.match_index = last_index();  // hint: retry from our end of log
    Send(std::move(r));
    return;
  }
  if (TermAt(a.prev_log_index) != a.prev_log_term) {
    // Skip back over the whole conflicting term in one round trip instead of
    // one entry at a time. Never hint below the commit index: those entries
    // are known to match.
    Term conflict_term = TermAt(a.prev_log_index);
    Index i = a.prev_log_index;
    while (i > commit_ + 1 && TermAt(i - 1) == conflict_term) --i;
    r.success = false;
    r.match_index = i - 1;
    Send(std::move(r));
    return;
  }

  // Append new entries, truncating our log at the first conflict. Entries we
  // already hold are skipped, so duplicated or reordered RPCs are harmless.
  std::vector<LogEntry> to_append;
  for (const auto& e : a.entries) {
    if (!to_append.empty()) {
      to_append.push_back(e);
      continue;
    }
    if (e.index <= last_index()) {
      if (TermAt(e.index) == e.term) continue;
      storage_.TruncateFrom(e.index);
    }
    to_append.push_back(e);
  }
  if (!to_append.empty()) storage_.Append(to_append);  // durable before ack

  Index last_new = a.prev_log_index + a.entries.size();
  if (a.leader_commit > commit_) commit_ = std::min(a.leader_commit, last_new);

  r.success = true;
  r.match_index = last_new;
  Send(std::move(r));
}

void RaftNode::HandleAppendEntriesResp(const Message& m) {
  if (role_ != Role::Leader) return;
  // Any response in our term proves this peer still follows us.
  active_.insert(m.from);
  if (m.last_log_index > peer_round_[m.from]) {
    peer_round_[m.from] = m.last_log_index;
    ConfirmReads();
  }
  if (m.success) {
    // Responses can arrive out of order; never move match_index backwards.
    match_index_[m.from] = std::max(match_index_[m.from], m.match_index);
    next_index_[m.from] = match_index_[m.from] + 1;
    MaybeAdvanceCommit();
    if (next_index_[m.from] <= last_index()) SendAppend(m.from);  // keep streaming
  } else {
    // Back up using the follower's hint, but never below what it has acked.
    Index next = std::min(next_index_[m.from], m.match_index + 1);
    next_index_[m.from] = std::max<Index>({next, match_index_[m.from] + 1, 1});
    SendAppend(m.from);
  }
}

void RaftNode::HandleInstallSnapshot(const Message& m) {
  // Same term as ours: the sender is this term's leader.
  if (role_ != Role::Follower) BecomeFollower(m.term, m.from);
  leader_ = m.from;
  ResetElectionTimer();

  Message r;
  r.type = MsgType::AppendEntriesResp;
  r.to = m.from;
  r.success = true;
  if (m.prev_log_index <= commit_) {
    // We already have (and committed) everything this snapshot covers.
    r.match_index = commit_;
    Send(std::move(r));
    return;
  }
  // Durable before acknowledging. The application restores its state machine
  // from it via TakeSnapshot() before applying any later entries.
  storage_.InstallSnapshot(m.prev_log_index, m.prev_log_term, m.snapshot);
  commit_ = m.prev_log_index;
  r.match_index = m.prev_log_index;
  Send(std::move(r));
}

// ---------------------------------------------------------------------------
// ReadIndex
// ---------------------------------------------------------------------------

bool RaftNode::RequestRead(uint64_t ctx) {
  if (role_ != Role::Leader) return false;
  if (TermAt(commit_) != term()) {
    // Until our no-op from this term commits, commit_ may lag entries that
    // earlier leaders already committed; wait for it (Raft thesis 6.4).
    reads_awaiting_term_commit_.push_back(ctx);
    return true;
  }
  StartReadRound({ctx});
  return true;
}

void RaftNode::StartReadRound(std::vector<uint64_t> ctxs) {
  // Reads at commit_ are linearizable once a quorum confirms we are still
  // leader *after* they were registered: a heartbeat round tagged with a new
  // round number, which followers echo.
  ++read_round_;
  for (uint64_t ctx : ctxs) reads_pending_.push_back({ctx, commit_, read_round_});
  if (Quorum() == 1) {
    ConfirmReads();
    return;
  }
  BroadcastAppend();
}

void RaftNode::ConfirmReads() {
  if (reads_pending_.empty()) return;
  std::vector<uint64_t> rounds;
  rounds.push_back(read_round_);  // ourselves
  for (NodeId p : peers_) rounds.push_back(peer_round_[p]);
  std::sort(rounds.begin(), rounds.end(), std::greater<uint64_t>());
  uint64_t confirmed = rounds[Quorum() - 1];  // highest round a quorum acked
  auto it = reads_pending_.begin();
  while (it != reads_pending_.end() && it->round <= confirmed) {
    ready_reads_.push_back({it->ctx, it->index});
    ++it;
  }
  reads_pending_.erase(reads_pending_.begin(), it);
}

std::vector<ConsensusNode::ReadState> RaftNode::TakeReadyReads() {
  std::vector<ReadState> out;
  out.swap(ready_reads_);
  return out;
}

void RaftNode::MaybeAdvanceCommit() {
  // Find the highest index replicated on a quorum whose entry is from the
  // current term (earlier-term entries commit indirectly via it).
  for (Index n = last_index(); n > commit_; --n) {
    if (TermAt(n) != term()) break;  // older terms can't be counted directly
    size_t replicas = 1;  // self
    for (const auto& [peer, match] : match_index_) {
      if (match >= n) ++replicas;
    }
    if (replicas >= Quorum()) {
      commit_ = n;
      if (!reads_awaiting_term_commit_.empty()) {
        std::vector<uint64_t> waiting;
        waiting.swap(reads_awaiting_term_commit_);
        StartReadRound(std::move(waiting));
      }
      return;
    }
  }
}

}  // namespace raftkv
