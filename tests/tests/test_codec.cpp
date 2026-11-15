#include "common/codec.h"
#include "common/crc32.h"
#include "kv/kv_store.h"
#include "net/net_util.h"
#include "test_framework.h"

using namespace raftkv;

TEST(Crc32KnownVector) {
  // Standard check value for CRC-32/IEEE.
  CHECK_EQ(crc32("123456789", 9), 0xCBF43926u);
}

TEST(MessageRoundTrip) {
  Message m;
  m.type = MsgType::AppendEntries;
  m.from = 1;
  m.to = 2;
  m.term = 7;
  m.prev_log_index = 10;
  m.prev_log_term = 6;
  m.leader_commit = 9;
  m.entries = {LogEntry{7, 11, "hello"}, LogEntry{7, 12, std::string("\0bin\xff", 5)}};
  CHECK(DecodeMessage(EncodeMessage(m)) == m);
}

TEST(DecodeRejectsTruncatedAndGarbageInput) {
  Message m;
  m.type = MsgType::AppendEntries;
  m.entries = {LogEntry{1, 1, "x"}};
  std::string bytes = EncodeMessage(m);
  for (size_t n = 0; n < bytes.size(); ++n) {
    bool threw = false;
    try {
      DecodeMessage(bytes.substr(0, n));
    } catch (const DecodeError&) {
      threw = true;
    }
    CHECK(threw);
  }
  bool threw = false;
  try {
    DecodeMessage(std::string(100, '\x09'));  // invalid type byte
  } catch (const DecodeError&) {
    threw = true;
  }
  CHECK(threw);
}

TEST(FramingHandlesPartialReads) {
  std::string stream = Frame("abc") + Frame("") + Frame("defg");
  std::string buf;
  std::vector<std::string> got;
  for (char ch : stream) {  // feed one byte at a time
    buf.push_back(ch);
    while (auto f = TryPopFrame(buf)) got.push_back(*f);
  }
  CHECK_EQ(got.size(), 3u);
  CHECK_EQ(got[0], std::string("abc"));
  CHECK_EQ(got[1], std::string(""));
  CHECK_EQ(got[2], std::string("defg"));
  CHECK(buf.empty());
}

TEST(FramingRejectsOversizedFrame) {
  Writer w;
  w.u32(kMaxFrameSize + 1);
  std::string buf = w.take();
  bool threw = false;
  try {
    TryPopFrame(buf);
  } catch (const DecodeError&) {
    threw = true;
  }
  CHECK(threw);
}

TEST(CommandAndResponseRoundTrip) {
  Command c{42, 7, OpType::Put, "key", "value"};
  CHECK(DecodeCommand(EncodeCommand(c)) == c);
  ClientResponse r{Status::NotLeader, 3, "v"};
  ClientResponse d = DecodeResponse(EncodeResponse(r));
  CHECK(d.status == r.status);
  CHECK_EQ(d.leader_hint, r.leader_hint);
  CHECK_EQ(d.value, r.value);
}

TEST(ParseClusterSpec) {
  auto nodes = ParseCluster("1=127.0.0.1:7001:8001,2=127.0.0.1:7002:8002");
  CHECK_EQ(nodes.size(), 2u);
  CHECK_EQ(nodes[1].id, 2u);
  CHECK_EQ(nodes[1].host, std::string("127.0.0.1"));
  CHECK_EQ(nodes[1].peer_port, 7002);
  CHECK_EQ(nodes[1].client_port, 8002);
}

#include <map>

#include "common/shard.h"

TEST(ShardMappingIsStableAndBalanced) {
  // Stable: clients and servers built separately must agree forever, so pin
  // known values (FNV-1a 64 test vectors).
  CHECK_EQ(Fnv1a64(""), 0xcbf29ce484222325ull);
  CHECK_EQ(Fnv1a64("a"), 0xaf63dc4c8601ec8cull);
  CHECK_EQ(ShardFor("anything", 1), 0u);

  // Balanced: 100K keys over 16 shards land within 5% of the mean per shard.
  std::map<uint32_t, int> counts;
  for (int i = 0; i < 100000; ++i) ++counts[ShardFor("user:" + std::to_string(i), 16)];
  CHECK_EQ(counts.size(), 16u);
  for (const auto& [shard, n] : counts) {
    CHECK(n > 6250 * 0.95 && n < 6250 * 1.05);
  }
}

TEST(MessageGroupIdRoundTrips) {
  Message m;
  m.type = MsgType::Accept;
  m.group = 1234567;
  CHECK_EQ(DecodeMessage(EncodeMessage(m)).group, 1234567u);
}
