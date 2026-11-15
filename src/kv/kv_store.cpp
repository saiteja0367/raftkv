#include "kv/kv_store.h"

#include <map>

#include "common/codec.h"

namespace raftkv {

const char* StatusName(Status s) {
  switch (s) {
    case Status::Ok: return "OK";
    case Status::NotFound: return "NOT_FOUND";
    case Status::NotLeader: return "NOT_LEADER";
    case Status::Retry: return "RETRY";
    case Status::Error: return "ERROR";
  }
  return "UNKNOWN";
}

std::string EncodeCommand(const Command& c) {
  Writer w;
  w.u64(c.client_id);
  w.u64(c.seq);
  w.u8(static_cast<uint8_t>(c.op));
  w.str(c.key);
  w.str(c.value);
  return w.take();
}

Command DecodeCommand(std::string_view data) {
  Reader r(data);
  Command c;
  c.client_id = r.u64();
  c.seq = r.u64();
  uint8_t op = r.u8();
  if (op < 1 || op > 3) throw DecodeError("unknown op");
  c.op = static_cast<OpType>(op);
  c.key = r.str();
  c.value = r.str();
  if (!r.done()) throw DecodeError("trailing bytes");
  return c;
}

std::string KvStore::Serialize() const {
  // Sorted, so identical states produce identical bytes on every replica.
  std::map<std::string, std::string> data(data_.begin(), data_.end());
  std::map<uint64_t, Session> sessions(sessions_.begin(), sessions_.end());
  Writer w;
  w.u64(data.size());
  for (const auto& [k, v] : data) {
    w.str(k);
    w.str(v);
  }
  w.u64(sessions.size());
  for (const auto& [id, s] : sessions) {
    w.u64(id);
    w.u64(s.last_seq);
    w.u8(static_cast<uint8_t>(s.last_result.status));
    w.str(s.last_result.value);
  }
  return w.take();
}

void KvStore::Restore(std::string_view snapshot) {
  Reader r(snapshot);
  std::unordered_map<std::string, std::string> data;
  std::unordered_map<uint64_t, Session> sessions;
  uint64_t n = r.u64();
  for (uint64_t i = 0; i < n; ++i) {
    std::string k = r.str();
    data[std::move(k)] = r.str();
  }
  uint64_t m = r.u64();
  for (uint64_t i = 0; i < m; ++i) {
    uint64_t id = r.u64();
    Session s;
    s.last_seq = r.u64();
    uint8_t st = r.u8();
    if (st > static_cast<uint8_t>(Status::Error)) throw DecodeError("bad status in snapshot");
    s.last_result.status = static_cast<Status>(st);
    s.last_result.value = r.str();
    sessions[id] = std::move(s);
  }
  if (!r.done()) throw DecodeError("trailing bytes in snapshot");
  data_ = std::move(data);
  sessions_ = std::move(sessions);
}

ApplyResult KvStore::Apply(const Command& c) {
  Session& s = sessions_[c.client_id];
  if (c.seq != 0 && c.seq <= s.last_seq) {
    // Duplicate (a client retry): return the original outcome, don't re-apply.
    return s.last_result;
  }

  ApplyResult res;
  switch (c.op) {
    case OpType::Put:
      data_[c.key] = c.value;
      break;
    case OpType::Get: {
      auto it = data_.find(c.key);
      if (it == data_.end()) {
        res.status = Status::NotFound;
      } else {
        res.value = it->second;
      }
      break;
    }
    case OpType::Delete:
      if (data_.erase(c.key) == 0) res.status = Status::NotFound;
      break;
  }

  if (c.seq != 0) {
    s.last_seq = c.seq;
    s.last_result = res;
  }
  return res;
}

}  // namespace raftkv
