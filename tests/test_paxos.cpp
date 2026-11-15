// Multi-Paxos-specific tests with full control over message delivery.

#include <map>
#include <memory>

#include "paxos/paxos_node.h"
#include "test_framework.h"

using namespace raftkv;

namespace {

// A tiny hand-driven cluster: messages are queued and delivered only when the
// test says so, so we can construct exact Paxos interleavings.
struct Manual {
  explicit Manual(int n) : n(n) {
    for (NodeId i = 1; i <= static_cast<NodeId>(n); ++i) {
      storage[i] = std::make_unique<MemoryPaxosStorage>();
      Start(i);
    }
  }
  void Start(NodeId i) {
    std::vector<NodeId> peers;
    for (NodeId p = 1; p <= static_cast<NodeId>(n); ++p) if (p != i) peers.push_back(p);
    node[i] = std::make_unique<PaxosNode>(i, peers, *storage[i], 1000 + i);
  }
  void Collect(NodeId i) {
    auto out = node[i]->TakeMessages();
    q.insert(q.end(), out.begin(), out.end());
  }
  // Delivers queued messages accepted by `keep` until the queue drains.
  template <typename Keep>
  void Pump(Keep keep) {
    for (int round = 0; round < 100 && !q.empty(); ++round) {
      std::vector<Message> batch;
      batch.swap(q);
      for (const auto& m : batch) {
        if (!keep(m) || !node[m.to]) continue;
        node[m.to]->Step(m);
        Collect(m.to);
        Record(m.to);
      }
    }
    q.clear();
  }
  void Pump() { Pump([](const Message&) { return true; }); }
  // Makes node i time out and run phase 1.
  void Campaign(NodeId i) {
    Ballot before = node[i]->ballot();
    while (!(node[i]->IsPreparing() && node[i]->ballot() != before) && !node[i]->IsLeader()) {
      node[i]->Tick();
    }
    Collect(i);
  }
  void Record(NodeId i) {
    if (auto snap = node[i]->TakeSnapshot()) snapshots[i] = *snap;  // restore first
    for (const auto& e : node[i]->TakeCommitted()) {
      auto [it, inserted] = committed.emplace(e.index, e.data);
      CHECK(inserted || it->second == e.data);
    }
  }

  int n;
  std::map<NodeId, std::unique_ptr<MemoryPaxosStorage>> storage;
  std::map<NodeId, std::unique_ptr<PaxosNode>> node;
  std::vector<Message> q;
  std::map<Index, std::string> committed;
  std::map<NodeId, Snapshot> snapshots;  // last snapshot each node restored
};

}  // namespace

TEST(PaxosBallotsAreUniqueAndOrdered) {
  CHECK(MakeBallot(2, 1) > MakeBallot(1, 9));
  CHECK(MakeBallot(1, 2) > MakeBallot(1, 1));
  CHECK_EQ(BallotRound(MakeBallot(7, 3)), 7u);
  CHECK_EQ(BallotNode(MakeBallot(7, 3)), 3u);
}

TEST(PaxosAcceptorRejectsLowerBallotAndPromiseSurvivesRestart) {
  MemoryPaxosStorage st;
  {
    PaxosNode a(1, {2, 3}, st, 1);
    Message prep;
    prep.type = MsgType::Prepare;
    prep.from = 2;
    prep.to = 1;
    prep.term = MakeBallot(5, 2);
    prep.prev_log_index = 1;
    a.Step(prep);
    auto out = a.TakeMessages();
    CHECK_EQ(out.size(), 1u);
    CHECK(out[0].success);
  }
  PaxosNode restarted(1, {2, 3}, st, 2);  // crash: only storage survives
  Message acc;
  acc.type = MsgType::Accept;
  acc.from = 3;
  acc.to = 1;
  acc.term = MakeBallot(4, 3);  // lower than the persisted promise
  acc.prev_log_index = 1;
  acc.entries = {LogEntry{acc.term, 1, "stale"}};
  restarted.Step(acc);
  auto out = restarted.TakeMessages();
  CHECK_EQ(out.size(), 1u);
  CHECK(!out[0].success);
  CHECK_EQ(out[0].term, MakeBallot(5, 2));  // tells the proposer what to beat
  CHECK(st.accepted().empty());
}

// The heart of Paxos: a value chosen by a majority must survive a leader
// crash, even if the old leader never told anyone it was chosen. Here S1
// gets "v" accepted by S1 and S2 (a majority of 3), then crashes before any
// commit index propagates. S3, which never saw "v", becomes leader: phase 1
// must recover "v" from S2's promise instead of using its own proposal.
TEST(PaxosNewLeaderRecoversChosenValueItNeverSaw) {
  Manual c(3);
  c.Campaign(1);
  c.Pump();
  CHECK(c.node[1]->IsLeader());

  c.node[1]->Propose("v");
  c.Collect(1);
  // Only S2 receives the Accept; S2's reply is lost; S3 sees nothing.
  c.Pump([](const Message& m) { return m.type == MsgType::Accept && m.to == 2; });
  CHECK_EQ(c.storage[2]->accepted().at(1).data, std::string("v"));
  CHECK(c.storage[3]->accepted().empty());

  c.node[1].reset();  // S1 crashes
  c.Campaign(3);
  c.Pump();
  CHECK(c.node[3]->IsLeader());
  c.node[3]->Propose("w");  // S3's own new command must go to a later slot
  c.Collect(3);
  for (int t = 0; t < 5; ++t) {
    c.node[3]->Tick();
    c.Collect(3);
    c.Pump();
  }
  c.Record(3);
  CHECK_EQ(c.committed.at(1), std::string("v"));
  CHECK_EQ(c.committed.at(2), std::string("w"));
}

// Acceptors can end up with holes (slot 2 never accepted anywhere). A new
// leader must fill holes with no-ops so later slots can be committed in order.
TEST(PaxosNewLeaderFillsHolesWithNoOps) {
  Manual c(3);
  c.storage[1]->Accept({LogEntry{MakeBallot(1, 1), 1, "a"}, LogEntry{MakeBallot(1, 1), 3, "c"}});
  c.storage[1]->SetPromised(MakeBallot(1, 1));
  c.Start(1);  // reload S1 with that state
  c.Campaign(2);
  c.Pump([](const Message& m) { return m.from != 3 && m.to != 3; });  // quorum = {S1, S2}
  CHECK(c.node[2]->IsLeader());
  for (int t = 0; t < 5; ++t) {
    c.node[2]->Tick();
    c.Collect(2);
    c.Pump();
  }
  c.Record(2);
  CHECK_EQ(c.committed.at(1), std::string("a"));
  CHECK_EQ(c.committed.at(2), std::string(""));  // no-op
  CHECK_EQ(c.committed.at(3), std::string("c"));
}

// If two acceptors accepted different values for a slot under different
// ballots, the new leader must pick the one with the HIGHER ballot: only it
// can have been chosen.
TEST(PaxosRecoveryPicksHighestBallotValue) {
  Manual c(5);
  c.storage[1]->Accept({LogEntry{MakeBallot(1, 1), 1, "old"}});
  c.storage[2]->Accept({LogEntry{MakeBallot(2, 4), 1, "newer"}});
  c.storage[1]->SetPromised(MakeBallot(1, 1));
  c.storage[2]->SetPromised(MakeBallot(2, 4));
  c.Start(1);
  c.Start(2);
  // S3's first ballot (round 1) is below S2's promise (round 2) and gets
  // rejected; S3 learns the higher round and retries with round 3.
  for (int attempt = 0; attempt < 3 && !c.node[3]->IsLeader(); ++attempt) {
    c.Campaign(3);
    c.Pump([](const Message& m) { return m.from <= 3 && m.to <= 3; });  // quorum = {S1, S2, S3}
  }
  CHECK(c.node[3]->IsLeader());
  CHECK(BallotRound(c.node[3]->ballot()) >= 3u);
  for (int t = 0; t < 5; ++t) {
    c.node[3]->Tick();
    c.Collect(3);
    c.Pump();
  }
  c.Record(3);
  CHECK_EQ(c.committed.at(1), std::string("newer"));
}

// Two nodes campaign at once. Higher ballots must win cleanly: the loser
// steps down and the cluster still converges on a single sequence of values.
TEST(PaxosDuelingProposersConverge) {
  Manual c(3);
  c.Campaign(1);
  c.Campaign(2);  // both send Prepare before anything is delivered
  c.Pump();
  int leaders = (c.node[1]->IsLeader() ? 1 : 0) + (c.node[2]->IsLeader() ? 1 : 0);
  CHECK(leaders <= 1);
  // Whoever leads (after more ticks if needed) can commit.
  for (int t = 0; t < 60 && !(c.node[1]->IsLeader() || c.node[2]->IsLeader() || c.node[3]->IsLeader()); ++t) {
    for (NodeId i = 1; i <= 3; ++i) {
      c.node[i]->Tick();
      c.Collect(i);
    }
    c.Pump();
  }
  NodeId leader = c.node[1]->IsLeader() ? 1 : c.node[2]->IsLeader() ? 2 : 3;
  CHECK(c.node[leader]->IsLeader());
  c.node[leader]->Propose("x");
  c.Collect(leader);
  for (int t = 0; t < 5; ++t) {
    c.node[leader]->Tick();
    c.Collect(leader);
    c.Pump();
  }
  c.Record(leader);
  CHECK_EQ(c.committed.at(1), std::string("x"));
}

// A new leader that is behind must adopt a snapshot reported in a promise.
// S1 and S2 chose and compacted slots 1..3; S3 never learned them. When S3
// campaigns, the promises no longer contain those slots' values. Without
// adopting the snapshot, S3 would treat slots 1..3 as holes and fill them
// with no-ops, overwriting chosen values.
TEST(PaxosLeaderAdoptsSnapshotFromPromise) {
  Manual c(3);
  c.Campaign(1);
  c.Pump([](const Message& m) { return m.from != 3 && m.to != 3; });
  CHECK(c.node[1]->IsLeader());
  for (const char* v : {"a", "b", "c"}) c.node[1]->Propose(v);
  c.Collect(1);
  for (int t = 0; t < 4; ++t) {
    c.node[1]->Tick();
    c.Collect(1);
    c.node[2]->Tick();
    c.Collect(2);
    c.Pump([](const Message& m) { return m.from != 3 && m.to != 3; });
  }
  c.Record(1);
  c.Record(2);
  CHECK(c.node[1]->commit_index() >= 3u && c.node[2]->commit_index() >= 3u);
  c.node[1]->Compact(3, "STATE-AFTER-abc");
  c.node[2]->Compact(3, "STATE-AFTER-abc");
  CHECK(c.storage[2]->accepted().empty());  // individual values are gone

  c.node[1].reset();  // S1 crashes; S3 (empty) campaigns with S2's promise
  c.Campaign(3);
  c.Pump();
  CHECK(c.node[3]->IsLeader());
  c.Record(3);
  CHECK(c.snapshots.count(3));
  CHECK_EQ(c.snapshots[3].index, 3u);
  CHECK_EQ(c.snapshots[3].data, std::string("STATE-AFTER-abc"));
  c.node[3]->Propose("d");
  c.Collect(3);
  for (int t = 0; t < 4; ++t) {
    c.node[3]->Tick();
    c.Collect(3);
    c.Pump();
  }
  c.Record(3);
  CHECK_EQ(c.committed.at(4), std::string("d"));  // new value goes after the snapshot
}
