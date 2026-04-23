#pragma once

#include <string>
#include <mutex>
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/arena.h"
#include "../util/iterator.h"
#include "skiplist.h"
#include "write_batch.h"

namespace lsm {

class MemTable {
 public:
  struct KeyComparator {
    int operator()(const Slice& a, const Slice& b) const;
  };

  MemTable();
  ~MemTable();

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void Put(const Slice& key, const Slice& value);
  void Delete(const Slice& key);
  void Write(const WriteBatch& batch);

  bool Get(const Slice& key, std::string* value) const;

  size_t ApproximateMemoryUsage() const;

  class Iterator : public lsm::Iterator {
   public:
    explicit Iterator(const MemTable* table);
    ~Iterator() override;

    bool Valid() const override;
    Slice key() const override;
    Slice value() const override;
    void Next() override;
    void Prev() override;
    void Seek(const Slice& key) override;
    void SeekToFirst() override;
    void SeekToLast() override;
    Status status() const override { return Status::OK(); }

   private:
    SkipList<Slice, KeyComparator>::Iterator iter_;
  };

  Iterator* NewIterator() const;

 private:
  friend class ImmutableMemTable;

  mutable Arena arena_;
  SkipList<Slice, KeyComparator> table_;
  mutable std::mutex mutex_;
  size_t memory_usage_;
};

}  // namespace lsm
