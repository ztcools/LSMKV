cmake_minimum_required(VERSION 3.16)
project(LSM-Tree VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wpedantic -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

option(ENABLE_ASAN "Enable Address Sanitizer" OFF)
option(ENABLE_UBSAN "Enable Undefined Behavior Sanitizer" OFF)
option(ENABLE_TSAN "Enable Thread Sanitizer" OFF)

if(ENABLE_ASAN)
  add_compile_options(-fsanitize=address -fno-optimize-sibling-calls)
  add_link_options(-fsanitize=address)
endif()

if(ENABLE_UBSAN)
  add_compile_options(-fsanitize=undefined)
  add_link_options(-fsanitize=undefined)
endif()

if(ENABLE_TSAN)
  add_compile_options(-fsanitize=thread)
  add_link_options(-fsanitize=thread)
endif()

find_package(Threads REQUIRED)

set(SRC_FILES
  src/util/status.cc
  src/util/slice.cc
  src/util/arena.cc
  src/util/util.cc
  src/util/cache.cc
  src/util/bloom_filter.cc
  src/util/options.cc
  src/util/metrics.cc
  src/memtable/write_batch.cc
  src/memtable/memtable.cc
  src/memtable/immutable_memtable.cc
  src/wal/wal.cc
  src/sstable/block.cc
  src/sstable/table_builder.cc
  src/sstable/table.cc
  src/sstable/table_cache.cc
  src/version/version.cc
  src/compaction/merge_iterator.cc
  src/compaction/compaction.cc
  src/db.cc
)

add_library(lsm STATIC ${SRC_FILES})

target_include_directories(lsm
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(lsm PUBLIC Threads::Threads)

# 工具程序
add_executable(sst_dump tools/sst_dump.cc)
target_link_libraries(sst_dump lsm)

add_executable(manifest_dump tools/manifest_dump.cc)
target_link_libraries(manifest_dump lsm)

add_executable(db_bench tools/db_bench.cc)
target_link_libraries(db_bench lsm)

# 测试程序
add_executable(simple_test test/simple_test.cc)
target_link_libraries(simple_test lsm)

add_executable(minimal_test test/minimal_test.cc)
target_link_libraries(minimal_test lsm)
