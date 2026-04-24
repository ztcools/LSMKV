#pragma once

#include <cstddef>
#include <vector>
#include <cassert>

namespace lsm {

class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  char* Allocate(size_t bytes);

  char* AllocateAligned(size_t bytes);

  size_t MemoryUsage() const {
    return memory_usage_;
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  std::vector<char*> blocks_;

  size_t memory_usage_;
};

inline char* Arena::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace lsm
