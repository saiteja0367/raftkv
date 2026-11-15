#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <random>

#include "storage/file_storage.h"
#include "storage/fs_util.h"
#include "test_framework.h"

using namespace raftkv;
namespace fs = std::filesystem;

namespace {

struct TempDir {
  fs::path path;
  TempDir() {
    std::random_device rd;
    path = fs::temp_directory_path() / ("raftkv-test-" + std::to_string(rd()) + std::to_string(rd()));
    fs::create_directories(path);
  }
  ~TempDir() { fs::remove_all(path); }
  std::string str() const { return path.string(); }
};

std::vector<LogEntry> Entries(Index from, Index to, Term term) {
  std::vector<LogEntry> v;
  for (Index i = from; i <= to; ++i) v.push_back({term, i, "value-" + std::to_string(i)});
  return v;
}

}  // namespace

TEST(FileStoragePersistsAcrossReopen) {
  TempDir dir;
  {
    FileStorage s(dir.str());
    s.SetHardState({5, 2});
    s.Append(Entries(1, 3, 4));
    s.Append(Entries(4, 5, 5));
  }
  FileStorage s(dir.str());
  CHECK(s.hard_state() == (HardState{5, 2}));
  CHECK_EQ(s.last_index(), 5u);
  CHECK_EQ(s.entries()[3].term, 5u);
  CHECK_EQ(s.entries()[4].data, std::string("value-5"));
  CHECK_EQ(s.recovered_tail_bytes(), 0u);
}

TEST(FileStorageTruncateThenAppendSurvivesReopen) {
  TempDir dir;
  {
    FileStorage s(dir.str());
    s.Append(Entries(1, 10, 1));
    s.TruncateFrom(6);  // conflict: drop 6..10
    s.Append(Entries(6, 7, 2));
  }
  FileStorage s(dir.str());
  CHECK_EQ(s.last_index(), 7u);
  CHECK_EQ(s.entries()[4].term, 1u);
  CHECK_EQ(s.entries()[5].term, 2u);
  CHECK_EQ(s.entries()[6].term, 2u);
}

TEST(FileStorageRecoversFromTornWrite) {
  // Simulate a crash in the middle of writing a record: chop bytes off the
  // end of the log at every possible position and check that recovery keeps
  // exactly the complete records and that appends work afterwards.
  TempDir dir;
  {
    FileStorage s(dir.str());
    s.Append(Entries(1, 3, 1));
  }
  fs::path log = dir.path / "log";
  auto full = fs::file_size(log);
  uintmax_t two_records = 0;
  {
    TempDir probe;
    FileStorage s(probe.str());
    s.Append(Entries(1, 2, 1));
    two_records = fs::file_size(probe.path / "log");
  }
  for (uintmax_t cut = two_records + 1; cut < full; ++cut) {
    TempDir copy;
    fs::copy_file(log, copy.path / "log");
    fs::resize_file(copy.path / "log", cut);
    FileStorage s(copy.str());
    CHECK_EQ(s.last_index(), 2u);
    CHECK_EQ(s.recovered_tail_bytes(), static_cast<size_t>(cut - two_records));
    s.Append(Entries(3, 3, 9));
    CHECK_EQ(s.entries()[2].term, 9u);
  }
}

TEST(FileStorageDetectsCorruptRecord) {
  TempDir dir;
  {
    FileStorage s(dir.str());
    s.Append(Entries(1, 5, 1));
  }
  // Flip one byte inside the 4th record's payload: CRC check must reject it
  // and everything after it.
  fs::path log = dir.path / "log";
  std::fstream f(log, std::ios::in | std::ios::out | std::ios::binary);
  std::string data((std::istreambuf_iterator<char>(f)), {});
  size_t pos = data.find("value-4");
  CHECK(pos != std::string::npos);
  f.seekp(static_cast<std::streamoff>(pos));
  f.put('X');
  f.close();

  FileStorage s(dir.str());
  CHECK_EQ(s.last_index(), 3u);
  CHECK(s.recovered_tail_bytes() > 0);
}

// --- Multi-Paxos acceptor storage -------------------------------------------

#include "paxos/paxos_storage.h"

TEST(FilePaxosStoragePersistsPromiseAndLatestAcceptance) {
  TempDir dir;
  {
    FilePaxosStorage s(dir.str());
    s.SetPromised(MakeBallot(3, 2));
    s.Accept({LogEntry{MakeBallot(1, 1), 1, "a"}, LogEntry{MakeBallot(1, 1), 2, "b"}});
    s.Accept({LogEntry{MakeBallot(3, 2), 2, "b2"}});  // slot 2 overwritten by a higher ballot
  }
  FilePaxosStorage s(dir.str());
  CHECK_EQ(s.promised(), MakeBallot(3, 2));
  CHECK_EQ(s.accepted().size(), 2u);
  CHECK_EQ(s.accepted().at(2).data, std::string("b2"));
  CHECK_EQ(s.accepted().at(2).term, MakeBallot(3, 2));
  CHECK_EQ(s.recovered_tail_bytes(), 0u);
}

TEST(FilePaxosStorageRecoversFromTornWrite) {
  TempDir dir;
  {
    FilePaxosStorage s(dir.str());
    s.Accept({LogEntry{1, 1, "first"}});
    s.Accept({LogEntry{1, 2, "second"}});
  }
  fs::path log = dir.path / "accepted";
  fs::resize_file(log, fs::file_size(log) - 3);  // crash mid-way through record 2
  FilePaxosStorage s(dir.str());
  CHECK_EQ(s.accepted().size(), 1u);
  CHECK_EQ(s.accepted().at(1).data, std::string("first"));
  CHECK(s.recovered_tail_bytes() > 0);
  s.Accept({LogEntry{2, 2, "again"}});  // appends work after recovery
  CHECK_EQ(s.accepted().at(2).data, std::string("again"));
}

// --- Shared node WAL (all groups in one file, one fsync per batch) ----------

#include "storage/node_wal.h"

TEST(NodeWalReplaysInterleavedGroups) {
  TempDir dir;
  {
    NodeWal wal(dir.str());
    WalRaftStorage g0(wal, 0);
    WalRaftStorage g1(wal, 1);
    WalPaxosStorage g2(wal, 2);
    g0.SetHardState({3, 1});
    g0.Append(Entries(1, 4, 3));
    g1.Append(Entries(1, 2, 1));
    g2.SetPromised(MakeBallot(5, 2));
    g2.Accept({LogEntry{MakeBallot(5, 2), 7, "x"}});
    g0.TruncateFrom(3);  // conflict in group 0 only
    g0.Append(Entries(3, 3, 4));
    wal.Sync();
    CHECK_EQ(wal.syncs(), 1u);  // all three groups, one fdatasync
  }
  NodeWal wal(dir.str());
  WalRaftStorage g0(wal, 0);
  WalRaftStorage g1(wal, 1);
  WalPaxosStorage g2(wal, 2);
  CHECK(g0.hard_state() == (HardState{3, 1}));
  CHECK_EQ(g0.last_index(), 3u);
  CHECK_EQ(g0.entries()[2].term, 4u);
  CHECK_EQ(g1.last_index(), 2u);
  CHECK_EQ(g2.promised(), MakeBallot(5, 2));
  CHECK_EQ(g2.accepted().at(7).data, std::string("x"));
  CHECK_EQ(wal.recovered_tail_bytes(), 0u);
}

TEST(NodeWalUnsyncedRecordsAreNotDurable) {
  // Records become durable only at Sync(); the server never releases a
  // message or reply before that barrier. A crash before Sync loses them,
  // which is safe precisely because nobody was told about them.
  TempDir dir;
  {
    NodeWal wal(dir.str());
    WalRaftStorage g(wal, 0);
    g.Append(Entries(1, 2, 1));
    wal.Sync();
    g.Append(Entries(3, 3, 1));  // buffered only
    CHECK(wal.dirty());
    // Simulate a crash: skip the destructor's final Sync.
    std::string path = dir.str() + "/wal";
    fs::copy_file(path, dir.path / "wal.crash");
  }
  fs::rename(dir.path / "wal.crash", dir.path / "wal");
  NodeWal wal(dir.str());
  WalRaftStorage g(wal, 0);
  CHECK_EQ(g.last_index(), 2u);
}

TEST(NodeWalRecoversFromTornTail) {
  TempDir dir;
  {
    NodeWal wal(dir.str());
    WalRaftStorage g(wal, 0);
    g.Append(Entries(1, 1, 1));
    wal.Sync();
    g.Append(Entries(2, 2, 1));
    wal.Sync();
  }
  fs::path p = dir.path / "wal";
  fs::resize_file(p, fs::file_size(p) - 5);
  NodeWal wal(dir.str());
  WalRaftStorage g(wal, 0);
  CHECK_EQ(g.last_index(), 1u);
  CHECK(wal.recovered_tail_bytes() > 0);
  g.Append(Entries(2, 2, 7));
  wal.Sync();
  CHECK_EQ(g.entries()[1].term, 7u);
}

// --- Snapshots in every storage engine --------------------------------------

TEST(FileStorageCompactionSurvivesReopenAndCrashMidway) {
  TempDir dir;
  {
    FileStorage s(dir.str());
    s.Append(Entries(1, 10, 2));
    s.Compact(6, 2, "snap@6");
    CHECK_EQ(s.first_index(), 7u);
    s.Append(Entries(11, 12, 3));
  }
  {
    FileStorage s(dir.str());
    CHECK_EQ(s.snapshot_index(), 6u);
    CHECK_EQ(s.snapshot_data(), std::string("snap@6"));
    CHECK_EQ(s.last_index(), 12u);
    CHECK_EQ(s.At(7).data, std::string("value-7"));
  }
  // Crash after writing a newer snapshot but before rewriting the log: the
  // old log still holds covered entries. Recovery must drop exactly those.
  {
    FileStorage s(dir.str());
    std::string old_log;
    CHECK(fsutil::ReadFile(dir.str() + "/log", old_log));
    s.Compact(9, 2, "snap@9");
    fsutil::AtomicWrite(dir.str(), "log", old_log);  // undo step 2
  }
  FileStorage s(dir.str());
  CHECK_EQ(s.snapshot_index(), 9u);
  CHECK_EQ(s.first_index(), 10u);
  CHECK_EQ(s.last_index(), 12u);
  CHECK_EQ(s.At(12).term, 3u);
}

TEST(InstallSnapshotDropsConflictingLog) {
  MemoryStorage s;
  s.Append(Entries(1, 8, 1));
  s.InstallSnapshot(5, 4, "leader-state");  // our entry 5 has term 1, not 4
  CHECK_EQ(s.snapshot_index(), 5u);
  CHECK_EQ(s.last_index(), 5u);  // suffix discarded: it doesn't extend the snapshot
  MemoryStorage t;
  t.Append(Entries(1, 8, 1));
  t.InstallSnapshot(5, 1, "leader-state");  // matches: keep 6..8
  CHECK_EQ(t.last_index(), 8u);
}

TEST(FilePaxosStorageSnapshotSurvivesReopen) {
  TempDir dir;
  {
    FilePaxosStorage s(dir.str());
    s.Accept({LogEntry{1, 1, "a"}, LogEntry{1, 2, "b"}, LogEntry{1, 3, "c"}});
    s.SetSnapshot(2, "snap@2");
  }
  FilePaxosStorage s(dir.str());
  CHECK_EQ(s.snapshot_index(), 2u);
  CHECK_EQ(s.snapshot_data(), std::string("snap@2"));
  CHECK_EQ(s.accepted().size(), 1u);
  CHECK_EQ(s.accepted().at(3).data, std::string("c"));
}

TEST(NodeWalSnapshotsAndCheckpointGarbageCollection) {
  TempDir dir;
  uint64_t before = 0, after = 0;
  {
    NodeWal wal(dir.str());
    wal.set_checkpoint_threshold(4096);
    WalRaftStorage r(wal, 0);
    WalPaxosStorage p(wal, 1);
    for (Index i = 1; i <= 200; ++i) {
      r.Append({LogEntry{1, i, std::string(40, 'x')}});
      p.Accept({LogEntry{1, i, std::string(40, 'y')}});
      wal.Sync();
    }
    r.Compact(190, 1, "raft-snap");
    p.SetSnapshot(195, "paxos-snap");
    wal.Sync();
    before = wal.size_bytes();
    CHECK(wal.MaybeCheckpoint());
    after = wal.size_bytes();
    CHECK(after < before / 10);  // only live state remains
    r.Append({LogEntry{2, 201, "after-checkpoint"}});
    wal.Sync();
  }
  NodeWal wal(dir.str());
  WalRaftStorage r(wal, 0);
  WalPaxosStorage p(wal, 1);
  CHECK_EQ(r.snapshot_index(), 190u);
  CHECK_EQ(r.snapshot_data(), std::string("raft-snap"));
  CHECK_EQ(r.last_index(), 201u);
  CHECK_EQ(r.At(201).data, std::string("after-checkpoint"));
  CHECK_EQ(p.snapshot_index(), 195u);
  CHECK_EQ(p.accepted().size(), 5u);
}
