#include "immutable_memtable.h"

namespace lsm {

ImmutableMemTable::ImmutableMemTable(std::unique_ptr<MemTable> table)
    : table_(std::move(table)) {}

ImmutableMemTable::~ImmutableMemTable() = default;

bool ImmutableMemTable::Get(const Slice& key, std::string* value) const {
  return table_->Get(key, value);
}

size_t ImmutableMemTable::ApproximateMemoryUsage() const {
  return table_->ApproximateMemoryUsage();
}

MemTable::Iterator* ImmutableMemTable::NewIterator() const {
  return table_->NewIterator();
}

}  // namespace lsm
