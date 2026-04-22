#pragma once

#include <string>
#include <mutex>
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/arena.h"
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

  class Iterator {
   public:
    explicit Iterator(const MemTable* table);
    ~Iterator();

    bool Valid() const;
    Slice key() const;
    Slice value() const;
    void Next();
    void Prev();
    void Seek(const Slice& key);
    void SeekToFirst();
    void SeekToLast();

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
