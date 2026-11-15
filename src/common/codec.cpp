#include "common/codec.h"

namespace raftkv {

void Writer::u32(uint32_t v) {
  for (int i = 0; i < 4; ++i) u8(static_cast<uint8_t>(v >> (8 * i)));
}

void Writer::u64(uint64_t v) {
  for (int i = 0; i < 8; ++i) u8(static_cast<uint8_t>(v >> (8 * i)));
}

void Writer::str(std::string_view s) {
  u32(static_cast<uint32_t>(s.size()));
  buf_.append(s);
}

void Reader::need(size_t n) const {
  if (remaining() < n) throw DecodeError("truncated input");
}

uint8_t Reader::u8() {
  need(1);
  return static_cast<uint8_t>(data_[pos_++]);
}

uint32_t Reader::u32() {
  need(4);
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_++])) << (8 * i);
  return v;
}

uint64_t Reader::u64() {
  need(8);
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(data_[pos_++])) << (8 * i);
  return v;
}

std::string Reader::str() {
  uint32_t n = u32();
  need(n);
  std::string s(data_.substr(pos_, n));
  pos_ += n;
  return s;
}

std::string EncodeMessage(const Message& m) {
  Writer w;
  w.u8(static_cast<uint8_t>(m.type));
  w.u32(m.group);
  w.u32(m.from);
  w.u32(m.to);
  w.u64(m.term);
  w.u64(m.last_log_index);
  w.u64(m.last_log_term);
  w.u64(m.prev_log_index);
  w.u64(m.prev_log_term);
  w.u64(m.leader_commit);
  w.u8(m.success ? 1 : 0);
  w.u64(m.match_index);
  w.u32(static_cast<uint32_t>(m.entries.size()));
  for (const auto& e : m.entries) {
    w.u64(e.term);
    w.u64(e.index);
    w.str(e.data);
  }
  w.str(m.snapshot);
  return w.take();
}

Message DecodeMessage(std::string_view bytes) {
  Reader r(bytes);
  Message m;
  uint8_t type = r.u8();
  if (type < 1 || type > kMaxMsgType) throw DecodeError("unknown message type");
  m.type = static_cast<MsgType>(type);
  m.group = r.u32();
  m.from = r.u32();
  m.to = r.u32();
  m.term = r.u64();
  m.last_log_index = r.u64();
  m.last_log_term = r.u64();
  m.prev_log_index = r.u64();
  m.prev_log_term = r.u64();
  m.leader_commit = r.u64();
  m.success = r.u8() != 0;
  m.match_index = r.u64();
  uint32_t n = r.u32();
  // Each entry needs at least 20 bytes; reject absurd counts before reserving.
  if (n > r.remaining() / 20) throw DecodeError("entry count exceeds payload");
  m.entries.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    LogEntry e;
    e.term = r.u64();
    e.index = r.u64();
    e.data = r.str();
    m.entries.push_back(std::move(e));
  }
  m.snapshot = r.str();
  if (!r.done()) throw DecodeError("trailing bytes");
  return m;
}

std::string Frame(std::string_view payload) {
  Writer w;
  w.u32(static_cast<uint32_t>(payload.size()));
  w.raw(payload);
  return w.take();
}

std::optional<std::string> TryPopFrame(std::string& buf) {
  if (buf.size() < 4) return std::nullopt;
  Reader r(std::string_view(buf).substr(0, 4));
  uint32_t len = r.u32();
  if (len > kMaxFrameSize) throw DecodeError("frame too large");
  if (buf.size() < 4 + static_cast<size_t>(len)) return std::nullopt;
  std::string payload = buf.substr(4, len);
  buf.erase(0, 4 + static_cast<size_t>(len));
  return payload;
}

}  // namespace raftkv
