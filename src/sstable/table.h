#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <memory>
#include <string>
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/iterator.h"
#include "../util/cache.h"
#include "../util/options.h"
#include "block.h"

namespace lsm {

class Block;

class TableCache;

// SSTable 读取器
class Table {
 public:
  static Status Open(const Options& options, const std::string& filename,
                     std::unique_ptr<Table>* table, std::shared_ptr<Cache> block_cache = nullptr);

  ~Table();

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  // 查找 key
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

  // 范围迭代
  std::unique_ptr<Iterator> NewIterator(const ReadOptions& options);

  // 检查 Key 是否可能存在
  bool KeyMayMatch(const Slice& key);

  uint64_t ApproximateOffsetOf(const Slice& key);

 private:
  friend class TableIterator;

  Table(const Options& options, const std::string& filename,
        std::unique_ptr<FilterBlockReader> filter,
        std::unique_ptr<Block> index_block,
        std::shared_ptr<Cache> cache);

  Status ReadBlock(const ReadOptions& options, const BlockHandle& handle,
                       std::unique_ptr<Block>* block) const;
  Status ReadRawBlock(const BlockHandle& handle, Slice* content,
                          CompressionType* type) const;

  const Options options_;
  const std::string filename_;
  mutable std::fstream file_;
  std::unique_ptr<FilterBlockReader> filter_;
  std::unique_ptr<Block> index_block_;
  std::shared_ptr<Cache> cache_;
  mutable std::mutex mutex_;
  uint64_t file_size_;
};

// 两阶段迭代器：先查 index，再查 data
class TableIterator : public Iterator {
 public:
  TableIterator(const Table* table, const ReadOptions& options);
  ~TableIterator();

  bool Valid() const override;
  Slice key() const override;
  Slice value() const override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  Status status() const override;

 private:
  void SetDataBlock();
  void SetDataBlockForSeek();
  void NextIndex();

  const Table* table_;
  ReadOptions options_;
  std::unique_ptr<Iterator> index_iter_;
  std::unique_ptr<Iterator> data_iter_;
  std::string data_block_key_;
  Status status_;
};

}  // namespace lsm
