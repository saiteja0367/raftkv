cmake_minimum_required(VERSION 3.16)
project(raftkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

option(RAFTKV_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)
if(RAFTKV_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

find_package(Threads REQUIRED)

# Core library: Raft, storage, KV state machine, networking.
add_library(raftkv_core
  src/common/crc32.cpp
  src/common/codec.cpp
  src/raft/raft_node.cpp
  src/paxos/paxos_node.cpp
  src/paxos/paxos_storage.cpp
  src/storage/file_storage.cpp
  src/storage/node_wal.cpp
  src/storage/fs_util.cpp
  src/kv/kv_store.cpp
  src/net/net_util.cpp
  src/net/server.cpp
  src/net/client.cpp
)
target_include_directories(raftkv_core PUBLIC src)
target_link_libraries(raftkv_core PUBLIC Threads::Threads)

add_executable(raftkv-server src/main_server.cpp)
target_link_libraries(raftkv-server PRIVATE raftkv_core)

add_executable(raftkv-cli src/main_cli.cpp)
target_link_libraries(raftkv-cli PRIVATE raftkv_core)

enable_testing()
add_executable(raftkv-tests
  tests/test_main.cpp
  tests/test_codec.cpp
  tests/test_file_storage.cpp
  tests/test_kv_store.cpp
  tests/test_raft.cpp
  tests/test_paxos.cpp
  tests/test_simulation.cpp
  tests/test_linearizability.cpp
)
target_link_libraries(raftkv-tests PRIVATE raftkv_core)
add_test(NAME raftkv-tests COMMAND raftkv-tests)
