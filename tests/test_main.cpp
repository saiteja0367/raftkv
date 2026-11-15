#include <chrono>
#include <cstring>

#include "test_framework.h"

int main(int argc, char** argv) {
  const char* filter = argc > 1 ? argv[1] : nullptr;
  int passed = 0, failed = 0;
  for (const auto& t : testing::Registry()) {
    if (filter && std::strstr(t.name, filter) == nullptr) continue;
    auto start = std::chrono::steady_clock::now();
    int before = testing::FailureCount();
    try {
      t.fn();
    } catch (const testing::AbortTest&) {
    } catch (const std::exception& e) {
      std::cerr << "  unexpected exception: " << e.what() << "\n";
      ++testing::FailureCount();
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - start).count();
    bool ok = testing::FailureCount() == before;
    std::cout << (ok ? "[ PASS ] " : "[ FAIL ] ") << t.name << " (" << ms << " ms)\n";
    (ok ? passed : failed)++;
  }
  std::cout << "\n" << passed << " passed, " << failed << " failed\n";
  return failed == 0 ? 0 : 1;
}
