#pragma once

#include <string>
#include <memory>
#include <mutex>
#include "../util/slice.h"
#include "../util/status.h"
#include "memtable.h"

namespace lsm {

class ImmutableMemTable {
 public:
  explicit ImmutableMemTable(std::unique_ptr<MemTable> table);
  ~ImmutableMemTable();

  ImmutableMemTable(const ImmutableMemTable&) = delete;
  ImmutableMemTable& operator=(const ImmutableMemTable&) = delete;

  bool Get(const Slice& key, std::string* value) const;

  size_t ApproximateMemoryUsage() const;

  MemTable::Iterator* NewIterator() const;

  const MemTable* GetMemTable() const { return table_.get(); }

 private:
  std::unique_ptr<MemTable> table_;
};

}  // namespace lsm
