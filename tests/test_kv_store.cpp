#include "kv/kv_store.h"
#include "test_framework.h"

using namespace raftkv;

TEST(KvPutGetDelete) {
  KvStore kv;
  CHECK(kv.Apply({1, 1, OpType::Put, "a", "1"}).status == Status::Ok);
  auto g = kv.Apply({1, 2, OpType::Get, "a", ""});
  CHECK(g.status == Status::Ok);
  CHECK_EQ(g.value, std::string("1"));
  CHECK(kv.Apply({1, 3, OpType::Delete, "a", ""}).status == Status::Ok);
  CHECK(kv.Apply({1, 4, OpType::Get, "a", ""}).status == Status::NotFound);
  CHECK(kv.Apply({1, 5, OpType::Delete, "a", ""}).status == Status::NotFound);
}

TEST(KvDeduplicatesRetriedRequests) {
  KvStore kv;
  // Client 1 writes x=1 (seq 1); client 2 then writes x=2.
  kv.Apply({1, 1, OpType::Put, "x", "1"});
  kv.Apply({2, 1, OpType::Put, "x", "2"});
  // Client 1's retry of seq 1 (its first reply was lost) arrives late.
  // It must NOT overwrite client 2's newer value.
  kv.Apply({1, 1, OpType::Put, "x", "1"});
  CHECK_EQ(kv.Apply({3, 1, OpType::Get, "x", ""}).value, std::string("2"));
}

TEST(KvDuplicateReturnsOriginalResult) {
  KvStore kv;
  kv.Apply({1, 1, OpType::Put, "k", "v"});
  auto first = kv.Apply({1, 2, OpType::Delete, "k", ""});
  auto retry = kv.Apply({1, 2, OpType::Delete, "k", ""});
  CHECK(first.status == Status::Ok);
  CHECK(retry.status == Status::Ok);  // not NotFound: same answer as the original
}
