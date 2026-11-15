#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/types.h"

namespace raftkv {

// Little-endian binary encoder for the wire protocol and the on-disk log.
class Writer {
 public:
  void u8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }
  void u32(uint32_t v);
  void u64(uint64_t v);
  void str(std::string_view s);  // u32 length prefix + bytes
  void raw(std::string_view s) { buf_.append(s); }

  const std::string& data() const { return buf_; }
  std::string take() { return std::move(buf_); }

 private:
  std::string buf_;
};

class DecodeError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// Bounds-checked decoder. Throws DecodeError on truncated or malformed input,
// so a corrupt network frame can never read past the end of its buffer.
class Reader {
 public:
  explicit Reader(std::string_view data) : data_(data) {}

  uint8_t u8();
  uint32_t u32();
  uint64_t u64();
  std::string str();

  bool done() const { return pos_ == data_.size(); }
  size_t remaining() const { return data_.size() - pos_; }

 private:
  void need(size_t n) const;

  std::string_view data_;
  size_t pos_ = 0;
};

std::string EncodeMessage(const Message& m);
Message DecodeMessage(std::string_view bytes);  // throws DecodeError

// Framing: every message on a TCP stream is [u32 length][payload].
constexpr uint32_t kMaxFrameSize = 16 * 1024 * 1024;
std::string Frame(std::string_view payload);

// If `buf` starts with a complete frame, extracts its payload, removes the
// frame from `buf`, and returns it. Returns nullopt if more bytes are needed.
// Throws DecodeError if the frame header announces an oversized frame.
std::optional<std::string> TryPopFrame(std::string& buf);

}  // namespace raftkv
