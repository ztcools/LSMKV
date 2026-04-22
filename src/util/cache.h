#pragma once

#include <cstdint>
#include "slice.h"

namespace lsm {

class Cache {
 public:
  Cache() = default;
  virtual ~Cache();

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  struct Handle {
    virtual ~Handle() = default;
    virtual void* Value() = 0;
  };

  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;
  virtual Handle* Lookup(const Slice& key) = 0;
  virtual void Release(Handle* handle) = 0;
  virtual void Erase(const Slice& key) = 0;
  virtual uint64_t NewId() = 0;

  virtual void Prune() {}

  virtual size_t TotalCharge() const = 0;
};

Cache* NewLRUCache(size_t capacity);

}  // namespace lsm
