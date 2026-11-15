#pragma once

// Small POSIX file helpers shared by the storage engines.

#include <string>

namespace raftkv::fsutil {

[[noreturn]] void Fail(const std::string& what);  // throws runtime_error with errno text
void WriteAll(int fd, const std::string& data);
std::string ReadAll(int fd);
void FsyncDir(const std::string& dir);
void MakeDir(const std::string& dir);

// Atomically replaces `path` with `data`: temp file -> fsync -> rename -> fsync dir.
void AtomicWrite(const std::string& dir, const std::string& name, const std::string& data);

// Reads a whole file. Returns false if it does not exist.
bool ReadFile(const std::string& path, std::string& out);

// Frames a payload as a checksummed record: [u32 len][u32 crc32][payload].
std::string Record(const std::string& payload);

}  // namespace raftkv::fsutil
