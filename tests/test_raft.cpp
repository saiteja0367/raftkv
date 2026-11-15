// Scenario tests for the Raft core, run on the deterministic simulator.

#include "kv/kv_store.h"
#include "raft/raft_node.h"
#include "sim_cluster.h"
#include "test_framework.h"

using namespace raftkv;
using sim::SimCluster;

namespace {

Command Put(uint64_t client, uint64_t seq, const std::string& k, const std::string& v) {
  return Command{client, seq, OpType::Put, k, v};
}

NodeId ElectLeader(SimCluster& c) {
  CHECK(c.RunUntil([&] { return c.Leader().has_value(); }, 500));
  return *c.Leader();
}

// Runs a scenario against every protocol; failures name the protocol.
template <typename Fn>
void ForEachProtocol(Fn fn) {
  for (auto p : {sim::Protocol::Raft, sim::Protocol::Paxos}) {
    try {
      fn(p);
    } catch (const ::testing::AbortTest&) {
      std::cerr << "  (protocol: " << sim::ProtocolName(p) << ")\n";
      throw;
    }
  }
}

}  // namespace

// ---------------------------------------------------------------------------
// Scenarios run against both Raft and Multi-Paxos.
// ---------------------------------------------------------------------------

TEST(SingleNodeClusterCommitsImmediately) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(1, 1, p);
    NodeId leader = ElectLeader(c);
    auto idx = c.Propose(leader, Put(1, 1, "a", "1"));
    CHECK(idx.has_value());
    CHECK(c.RunUntil([&] { return c.AllCommitted(*idx); }, 10));
    CHECK_EQ(c.kv(leader).data().at("a"), std::string("1"));
  });
}

TEST(ElectsExactlyOneStableLeader) {
  ForEachProtocol([](sim::Protocol p) {
    for (uint64_t seed = 1; seed <= 50; ++seed) {
      SimCluster c(3, seed, p);
      NodeId leader = ElectLeader(c);
      int leaders = 0;
      for (int i = 0; i < 3; ++i) {
        if (c.node(SimCluster::Id(i))->IsLeader()) ++leaders;
      }
      CHECK_EQ(leaders, 1);
      c.Run(200);  // stable cluster: leadership should not churn
      CHECK_EQ(*c.Leader(), leader);
    }
  });
}

TEST(ReplicatesToAllNodes) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(5, 7, p);
    NodeId leader = ElectLeader(c);
    Index last = 0;
    for (int i = 1; i <= 100; ++i) last = *c.Propose(leader, Put(1, i, "k" + std::to_string(i), "v"));
    CHECK(c.RunUntil([&] { return c.AllCommitted(last); }, 200));
    c.CheckReplicasConverged();
    CHECK_EQ(c.kv(1).size(), 100u);
  });
}

TEST(LeaderCrashTriggersFailoverWithoutLosingCommittedData) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(3, 11, p);
    NodeId old_leader = ElectLeader(c);
    uint64_t old_epoch = c.node(old_leader)->epoch();
    Index idx = *c.Propose(old_leader, Put(1, 1, "durable", "yes"));
    CHECK(c.RunUntil([&] { return c.AllCommitted(idx); }, 100));

    c.Crash(old_leader);
    CHECK(c.RunUntil([&] { return c.Leader().has_value(); }, 500));
    NodeId new_leader = *c.Leader();
    CHECK(new_leader != old_leader);
    CHECK(c.node(new_leader)->epoch() > old_epoch);

    // The new leader still has the committed write and can accept new ones.
    CHECK_EQ(c.kv(new_leader).data().at("durable"), std::string("yes"));
    Index idx2 = *c.Propose(new_leader, Put(1, 2, "after", "failover"));
    CHECK(c.RunUntil([&] { return c.node(new_leader)->commit_index() >= idx2; }, 100));

    // The old leader rejoins as a follower and catches up.
    c.Restart(old_leader);
    CHECK(c.RunUntil([&] { return c.AllCommitted(idx2); }, 500));
    c.CheckReplicasConverged();
  });
}

TEST(IsolatedLeaderCannotCommitAndItsWritesAreDiscarded) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(5, 3, p);
    NodeId old_leader = ElectLeader(c);

    // Cut the leader off from the majority.
    c.Partition({old_leader});
    Index stale = *c.Propose(old_leader, Put(1, 1, "x", "from-minority"));
    c.Run(300);
    CHECK(c.node(old_leader)->commit_index() < stale);  // no quorum => no commit

    // The majority side elects a new leader and commits a conflicting write.
    std::optional<NodeId> new_leader;
    CHECK(c.RunUntil([&] {
      for (int i = 0; i < 5; ++i) {
        NodeId id = SimCluster::Id(i);
        if (id != old_leader && c.node(id)->IsLeader()) new_leader = id;
      }
      return new_leader.has_value();
    }, 500));
    Index good = *c.Propose(*new_leader, Put(2, 1, "x", "from-majority"));
    CHECK(c.RunUntil([&] { return c.node(*new_leader)->commit_index() >= good; }, 200));

    // After healing, the old leader is deposed and converges to the majority.
    c.Heal();
    CHECK(c.RunUntil([&] { return c.AllCommitted(good); }, 500));
    c.CheckReplicasConverged();
    CHECK_EQ(c.kv(old_leader).data().at("x"), std::string("from-majority"));
  });
}

TEST(FollowerCatchesUpAfterLongOutage) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(3, 21, p);
    NodeId leader = ElectLeader(c);
    NodeId follower = leader == 1 ? 2 : 1;
    c.Crash(follower);
    Index last = 0;
    for (int i = 1; i <= 1000; ++i) {
      last = *c.Propose(leader, Put(1, i, "k" + std::to_string(i % 50), std::to_string(i)));
      if (i % 10 == 0) c.Tick();
    }
    c.Restart(follower);
    CHECK(c.RunUntil([&] { return c.AllCommitted(last); }, 2000));
    c.CheckReplicasConverged();
  });
}

// ---------------------------------------------------------------------------
// Raft-specific tests.
// ---------------------------------------------------------------------------

TEST(VoteIsPersistedAcrossRestart) {
  // A node that voted for candidate 2 in term 5 must not vote for candidate 3
  // in term 5 after a crash: the vote lives in Storage, not in memory.
  MemoryStorage storage;
  {
    RaftNode n(1, {2, 3}, storage, 1);
    Message rv;
    rv.type = MsgType::RequestVote;
    rv.from = 2;
    rv.to = 1;
    rv.term = 5;
    n.Step(rv);
    auto out = n.TakeMessages();
    CHECK_EQ(out.size(), 1u);
    CHECK(out[0].success);
  }
  RaftNode restarted(1, {2, 3}, storage, 2);
  Message rv;
  rv.type = MsgType::RequestVote;
  rv.from = 3;
  rv.to = 1;
  rv.term = 5;
  restarted.Step(rv);
  auto out = restarted.TakeMessages();
  CHECK_EQ(out.size(), 1u);
  CHECK(!out[0].success);
}

TEST(RejectsVoteForCandidateWithStaleLog) {
  MemoryStorage storage;
  storage.Append({LogEntry{3, 1, "a"}, LogEntry{3, 2, "b"}});
  storage.SetHardState({3, kNoNode});
  RaftNode n(1, {2, 3}, storage, 1);

  Message rv;
  rv.type = MsgType::RequestVote;
  rv.from = 2;
  rv.to = 1;
  rv.term = 4;
  rv.last_log_index = 5;  // longer log, but older last term => less up to date
  rv.last_log_term = 2;
  n.Step(rv);
  auto out = n.TakeMessages();
  CHECK_EQ(out.size(), 1u);
  CHECK(!out[0].success);
}

// Reproduces Figure 8 of the Raft paper step by step, with full control over
// which messages are delivered. A leader must NOT treat an entry from an
// earlier term as committed just because a majority now stores it: a later
// leader can legitimately overwrite it. Raft's rule is to only count replicas
// for entries of the leader's current term.
TEST(Figure8OldTermEntryIsNotCommittedByCountingReplicas) {
  RaftConfig cfg;
  cfg.max_entries_per_append = 1;  // send one entry per RPC, for precise control
  cfg.pre_vote = false;            // drive real elections directly
  cfg.check_quorum = false;

  // Initial logs (index: term). S5 holds a conflicting term-3 entry at index 2.
  MemoryStorage st[6];
  for (int i = 1; i <= 5; ++i) {
    st[i].Append({LogEntry{1, 1, ""}});
    st[i].SetHardState({3, kNoNode});
  }
  st[1].Append({LogEntry{2, 2, "from-term-2"}});
  st[2].Append({LogEntry{2, 2, "from-term-2"}});
  st[5].Append({LogEntry{3, 2, "from-term-3"}});

  std::unique_ptr<RaftNode> n[6];
  for (NodeId i = 1; i <= 5; ++i) {
    std::vector<NodeId> peers;
    for (NodeId p = 1; p <= 5; ++p) if (p != i) peers.push_back(p);
    n[i] = std::make_unique<RaftNode>(i, peers, st[i], 100 + i, cfg);
  }

  std::map<Index, LogEntry> committed;  // global record, as in the simulator
  auto record = [&](RaftNode& node) {
    for (const auto& e : node.TakeCommitted()) {
      auto [it, inserted] = committed.emplace(e.index, e);
      CHECK(inserted || it->second == e);  // the safety property under test
    }
  };
  // Delivers queued messages for which keep(m) is true, until none are left.
  std::vector<Message> q;
  auto pump = [&](auto keep) {
    for (int round = 0; round < 50 && !q.empty(); ++round) {
      std::vector<Message> batch;
      batch.swap(q);
      for (const auto& m : batch) {
        if (!keep(m) || !n[m.to]) continue;
        n[m.to]->Step(m);
        auto out = n[m.to]->TakeMessages();
        q.insert(q.end(), out.begin(), out.end());
        record(*n[m.to]);
      }
    }
    q.clear();
  };
  auto take = [&](NodeId id) {
    auto out = n[id]->TakeMessages();
    q.insert(q.end(), out.begin(), out.end());
  };

  // (1) S1 wins term 4 with votes from S2 and S3.
  while (n[1]->role() != Role::Candidate) n[1]->Tick();
  take(1);
  pump([](const Message& m) {
    bool vote = m.type == MsgType::RequestVote || m.type == MsgType::RequestVoteResp;
    return vote && m.to <= 3 && m.from <= 3;
  });
  CHECK(n[1]->role() == Role::Leader);
  CHECK_EQ(n[1]->term(), 4u);

  // (2) S1 replicates the term-2 entry (index 2) to S3, so S1, S2, S3 all
  // store it, a majority. S1's own term-4 no-op (index 3) reaches only S2.
  // S4 and S5 hear nothing. S1 must NOT consider index 2 committed.
  for (int t = 0; t < 2; ++t) n[1]->Tick();  // trigger a heartbeat round
  take(1);
  pump([&](const Message& m) {
    if (m.to == 4 || m.to == 5 || m.from == 4 || m.from == 5) return false;
    // Once S3 holds index 2, stop it from receiving index 3.
    bool carries_noop = !m.entries.empty() && m.entries[0].index >= 3;
    if (m.from == 1 && m.to == 3 && carries_noop && n[3]->last_index() >= 2) return false;
    return true;
  });
  CHECK_EQ(n[3]->last_index(), 2u);
  CHECK_EQ(n[3]->TermAt(2), 2u);
  CHECK_EQ(n[1]->commit_index(), 0u);  // the Figure 8 rule
  record(*n[1]);

  // (3) S1 crashes. S5 (last log term 3) wins term 5 with votes from S3, S4.
  n[1].reset();
  while (!(n[5]->role() == Role::Candidate && n[5]->term() == 5)) {
    n[5]->Tick();  // first candidacy is term 4 (already decided); time out again
    if (!(n[5]->role() == Role::Candidate && n[5]->term() == 5)) n[5]->TakeMessages();
  }
  take(5);
  pump([](const Message& m) {
    bool vote = m.type == MsgType::RequestVote || m.type == MsgType::RequestVoteResp;
    return vote && m.from != 2 && m.to != 2;
  });
  CHECK(n[5]->role() == Role::Leader);

  // (4) S5 overwrites index 2 on the others with its term-3 entry and
  // commits it. This is legal precisely because index 2 was never committed.
  for (int t = 0; t < 10; ++t) {
    n[5]->Tick();
    take(5);
    pump([](const Message&) { return true; });
  }
  record(*n[5]);
  CHECK(n[5]->commit_index() >= 2u);
  CHECK_EQ(committed.at(2).term, 3u);
  CHECK_EQ(n[2]->TermAt(2), 3u);
  CHECK_EQ(n[3]->TermAt(2), 3u);
}

// ---------------------------------------------------------------------------
// Snapshots and log compaction (both protocols).
// ---------------------------------------------------------------------------

TEST(LaggingFollowerCatchesUpViaSnapshot) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(3, 31, p);
    c.compact_every = 50;
    NodeId leader = ElectLeader(c);
    NodeId follower = leader == 1 ? 2 : 1;
    c.Crash(follower);
    Index last = 0;
    for (int i = 1; i <= 600; ++i) {
      last = *c.Propose(leader, Put(1, i, "k" + std::to_string(i % 40), std::to_string(i)));
      if (i % 10 == 0) c.Tick();
    }
    c.Run(20);
    // The leader compacted away the entries the follower missed...
    CHECK(c.node(leader)->snapshot_index() > 100);
    uint64_t before = c.snapshots_installed();
    c.Restart(follower);
    CHECK(c.RunUntil([&] { return c.AllCommitted(last); }, 2000));
    c.Run(20);
    // ...so it can only have caught up by installing a snapshot.
    CHECK(c.snapshots_installed() > before);
    c.CheckReplicasConverged();
    CHECK_EQ(c.kv(follower).data().at("k0"), std::string("600"));
  });
}

TEST(RestartedNodeRecoversFromItsOwnSnapshot) {
  ForEachProtocol([](sim::Protocol p) {
    SimCluster c(3, 41, p);
    c.compact_every = 30;
    NodeId leader = ElectLeader(c);
    Index last = 0;
    for (int i = 1; i <= 200; ++i) last = *c.Propose(leader, Put(1, i, "k" + std::to_string(i), "v"));
    CHECK(c.RunUntil([&] { return c.AllCommitted(last); }, 500));
    c.Run(10);
    // Restart every node: each rebuilds its KV state from its own snapshot
    // plus the log suffix, without anyone re-sending the full history.
    for (int i = 0; i < 3; ++i) c.Crash(SimCluster::Id(i));
    for (int i = 0; i < 3; ++i) c.Restart(SimCluster::Id(i));
    CHECK(c.RunUntil([&] { return c.Leader().has_value(); }, 500));
    c.Run(50);
    c.CheckReplicasConverged();
    CHECK_EQ(c.kv(1).size(), 200u);
  });
}

TEST(KvSnapshotRoundTripIncludesSessions) {
  KvStore kv;
  kv.Apply({7, 1, OpType::Put, "a", "1"});
  kv.Apply({7, 2, OpType::Put, "b", "2"});
  kv.Apply({9, 5, OpType::Delete, "a", ""});
  KvStore restored;
  restored.Restore(kv.Serialize());
  CHECK(restored.data() == kv.data());
  // Dedup state survived: a late retry of client 7's seq 1 must not re-apply.
  restored.Apply({7, 1, OpType::Put, "a", "1"});
  CHECK(restored.data().count("a") == 0);
  CHECK(kv.Serialize() == restored.Serialize());  // deterministic encoding
}

// ---------------------------------------------------------------------------
// Pre-vote and check-quorum.
// ---------------------------------------------------------------------------

namespace {

// Isolates one follower for a long time, heals, and reports whether the
// stable leader survived and how much the term moved.
std::pair<bool, Term> RejoinDisruption(bool pre_vote) {
  RaftConfig cfg;
  cfg.pre_vote = pre_vote;
  SimCluster c(5, 77, sim::Protocol::Raft, cfg);
  NodeId leader = ElectLeader(c);
  Term before = c.raft(leader)->term();
  NodeId loner = leader == 1 ? 2 : 1;
  c.Partition({loner});
  c.Run(600);  // the loner times out again and again
  c.Heal();
  c.Run(300);
  auto now = c.Leader();
  return {now && *now == leader, c.raft(*now)->term() - before};
}

}  // namespace

TEST(PreVoteStopsRejoiningNodeFromDisruptingLeader) {
  auto [kept_with, bump_with] = RejoinDisruption(/*pre_vote=*/true);
  CHECK(kept_with);           // same leader...
  CHECK_EQ(bump_with, 0u);    // ...and the cluster's term never moved
  // Without pre-vote the loner inflates its term while isolated, and its
  // first message after healing forces the healthy leader to step down.
  auto [kept_without, bump_without] = RejoinDisruption(/*pre_vote=*/false);
  CHECK(!kept_without || bump_without > 0);
  CHECK(bump_without > 10u);
}

TEST(CheckQuorumIsolatedLeaderStepsDown) {
  SimCluster c(5, 5, sim::Protocol::Raft);
  NodeId leader = ElectLeader(c);
  c.Partition({leader});
  // Within ~two election timeouts it notices it can't reach a majority.
  CHECK(c.RunUntil([&] { return !c.node(leader)->IsLeader(); }, 60));
  // Without check-quorum it would stay "leader" indefinitely.
  RaftConfig cfg;
  cfg.check_quorum = false;
  SimCluster d(5, 5, sim::Protocol::Raft, cfg);
  NodeId l2 = ElectLeader(d);
  d.Partition({l2});
  d.Run(300);
  CHECK(d.node(l2)->IsLeader());
}

TEST(ReadIndexServesReadsWithoutLogWrites) {
  SimCluster c(3, 9, sim::Protocol::Raft);
  NodeId leader = ElectLeader(c);
  Index idx = *c.Propose(leader, Put(1, 1, "k", "v"));
  CHECK(c.RunUntil([&] { return c.AllCommitted(idx); }, 100));
  Index last_before = c.raft(leader)->last_index();
  std::vector<uint64_t> served;
  c.on_read = [&](NodeId, uint64_t ctx) { served.push_back(ctx); };
  for (uint64_t ctx = 1; ctx <= 20; ++ctx) CHECK(c.RequestRead(leader, ctx));
  CHECK(c.RunUntil([&] { return served.size() == 20; }, 20));
  CHECK_EQ(c.raft(leader)->last_index(), last_before);  // no log entries for reads
  // A follower can't serve them (it isn't the leader).
  NodeId follower = leader == 1 ? 2 : 1;
  CHECK(!c.RequestRead(follower, 99));
}
