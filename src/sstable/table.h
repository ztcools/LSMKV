#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>
#include "../util/slice.h"
#include "../util/status.h"
#include "block.h"

namespace lsm {

struct ReadOptions {
  bool verify_checksums = false;
  bool fill_cache = true;
};

// 简单的 Block 缓存（LRU）
class BlockCache {
 public:
  explicit BlockCache(size_t capacity);
  ~BlockCache();

  Block* Lookup(const std::string& key);
  void Insert(const std::string& key, std::unique_ptr<Block> block);
  void Erase(const std::string& key);
  void Clear();

 private:
  struct LRUElement;
  std::unordered_map<std::string, std::unique_ptr<LRUElement>> cache_;
  LRUElement* head_;
  LRUElement* tail_;
  size_t capacity_;
  size_t usage_;
  std::mutex mutex_;
};

// SSTable 读取器
class Table {
 public:
  static Status Open(const Options& options, const std::string& filename,
                     std::unique_ptr<Table>* table);

  ~Table();

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  // 查找 key
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

  // 范围迭代
  class Iterator;
  Iterator* NewIterator(const ReadOptions& options);

  // 检查 Key 是否可能存在
  bool KeyMayMatch(const Slice& key);

  uint64_t ApproximateOffsetOf(const Slice& key);

 private:
  friend class Table::Iterator;

  Table(const Options& options, const std::string& filename,
        std::unique_ptr<FilterBlockReader> filter,
        std::unique_ptr<Block> index_block,
        std::unique_ptr<BlockCache> cache);

  Status ReadBlock(const ReadOptions& options, const BlockHandle& handle,
                   std::unique_ptr<Block>* block) const;
  Status ReadRawBlock(const BlockHandle& handle, Slice* content,
                      CompressionType* type) const;

  const Options options_;
  const std::string filename_;
  mutable std::fstream file_;
  std::unique_ptr<FilterBlockReader> filter_;
  std::unique_ptr<Block> index_block_;
  mutable std::unique_ptr<BlockCache> cache_;
  mutable std::mutex mutex_;
  uint64_t file_size_;
};

// 两阶段迭代器：先查 index，再查 data
class Table::Iterator {
 public:
  Iterator(const Table* table, const ReadOptions& options);
  ~Iterator();

  bool Valid() const;
  Slice key() const;
  Slice value() const;

  void Seek(const Slice& target);
  void SeekToFirst();
  void SeekToLast();
  void Next();
  void Prev();

  Status status() const;

 private:
  void SetDataBlock();
  void SetDataBlockForSeek();
  void NextIndex();

  const Table* table_;
  ReadOptions options_;
  std::unique_ptr<Block::Iterator> index_iter_;
  std::unique_ptr<Block::Iterator> data_iter_;
  std::string data_block_key_;
  Status status_;
};

}  // namespace lsm
