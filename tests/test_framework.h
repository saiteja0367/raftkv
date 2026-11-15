#pragma once

// A tiny dependency-free test framework: TEST() registers a function,
// CHECK()/CHECK_EQ() record failures with file:line, and the runner in
// test_main.cpp executes everything and reports a summary.

#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace testing {

struct TestCase {
  const char* name;
  std::function<void()> fn;
};

inline std::vector<TestCase>& Registry() {
  static std::vector<TestCase> r;
  return r;
}

inline int& FailureCount() {
  static int n = 0;
  return n;
}

struct Registrar {
  Registrar(const char* name, std::function<void()> fn) { Registry().push_back({name, std::move(fn)}); }
};

struct AbortTest {};

}  // namespace testing

#define TEST_CONCAT2(a, b) a##b
#define TEST_CONCAT(a, b) TEST_CONCAT2(a, b)

#define TEST(name)                                                              \
  static void name();                                                           \
  static ::testing::Registrar TEST_CONCAT(registrar_, name)(#name, name);       \
  static void name()

#define CHECK(cond)                                                             \
  do {                                                                          \
    if (!(cond)) {                                                              \
      std::cerr << "  " << __FILE__ << ":" << __LINE__ << ": CHECK(" #cond ") failed\n"; \
      ++::testing::FailureCount();                                              \
      throw ::testing::AbortTest{};                                             \
    }                                                                           \
  } while (0)

#define CHECK_EQ(a, b)                                                          \
  do {                                                                          \
    auto _va = (a);                                                             \
    auto _vb = (b);                                                             \
    if (!(_va == _vb)) {                                                        \
      std::ostringstream _os;                                                   \
      _os << "  " << __FILE__ << ":" << __LINE__ << ": CHECK_EQ(" #a ", " #b ") failed: " \
          << _va << " vs " << _vb << "\n";                                      \
      std::cerr << _os.str();                                                   \
      ++::testing::FailureCount();                                              \
      throw ::testing::AbortTest{};                                             \
    }                                                                           \
  } while (0)
