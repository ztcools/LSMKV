#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include "../util/slice.h"
#include "../util/status.h"

namespace lsm {

// 压缩类型
enum CompressionType {
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kLZ4Compression = 0x2,
  kZSTDCompression = 0x3
};

// SSTable 通用选项
struct Options {
  size_t block_size = 4096;        // Block 大小
  size_t block_restart_interval = 16;  // 前缀压缩 restart 间隔
  CompressionType compression = kNoCompression;  // 压缩类型
};

// Block句柄（位置 + 大小）
struct BlockHandle {
  static const size_t kMaxEncodedLength = 10 + 10;
  uint64_t offset;
  uint64_t size;

  BlockHandle() : offset(0), size(0) {}
  BlockHandle(uint64_t off, uint64_t sz) : offset(off), size(sz) {}

  void set_offset(uint64_t off) { offset = off; }
  void set_size(uint64_t sz) { size = sz; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};

// Footer：固定大小，定位元数据
// 格式：[meta index handle][index handle][padding][magic]
struct Footer {
  static const uint64_t kTableMagicNumber = 0xbc9f1d34;
  static const size_t kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8;

  BlockHandle metaindex_handle;
  BlockHandle index_handle;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};

// Block：通用数据块
class Block {
 public:
  explicit Block(const char* data, size_t size, bool compressed = false,
                 CompressionType type = kNoCompression);
  ~Block();

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  size_t size() const { return size_; }
  const char* data() const { return data_; }

  class Iterator;
  Iterator* NewIterator() const;

 private:
  friend class Block::Iterator;
  friend class TwoLevelIterator;

  const char* data_;
  size_t size_;
  bool compressed_;
  CompressionType compression_type_;
  const char* decompressed_;
  size_t decompressed_size_;

  uint32_t NumRestarts() const;
  const char* RestartPoint(uint32_t index) const;
};

// Block迭代器
class Block::Iterator {
 public:
  explicit Iterator(const Block* block);

  bool Valid() const;
  Slice key() const;
  Slice value() const;

  void Seek(const Slice& target);
  void SeekToFirst();
  void SeekToLast();
  void Next();
  void Prev();

  Status status() const { return status_; }

 private:
  const Block* block_;
  const char* data_end_;

  // 当前位置
  const char* current_;
  uint32_t restart_index_;

  // 解析出的key和value
  std::string key_;
  Slice value_;

  Status status_;

  bool ParseNextKey();
  uint32_t BinarySearchIndex(const Slice& target);
};

// 数据块构建器（支持前缀压缩）
class BlockBuilder {
 public:
  explicit BlockBuilder(size_t block_restart_interval = 16);
  ~BlockBuilder();

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  void Reset();
  void Add(const Slice& key, const Slice& value);
  Slice Finish();
  size_t CurrentSizeEstimate() const;
  bool empty() const { return buffer_.empty(); }

 private:
  const size_t block_restart_interval_;
  std::string buffer_;
  std::vector<uint32_t> restarts_;
  std::string last_key_;
  int counter_;
  bool finished_;
};

// 过滤器块构建器（BloomFilter）
class FilterBlockBuilder {
 public:
  FilterBlockBuilder();
  ~FilterBlockBuilder();

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const size_t kBitsPerKey = 10;
  std::string keys_;
  std::vector<size_t> key_offsets_;
  std::string result_;
  std::vector<uint32_t> filter_offsets_;
};

// 过滤器块读取器
class FilterBlockReader {
 public:
  explicit FilterBlockReader(const Slice& contents);
  ~FilterBlockReader();

  FilterBlockReader(const FilterBlockReader&) = delete;
  FilterBlockReader& operator=(const FilterBlockReader&) = delete;

  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const char* data_;
  const char* offset_;
  size_t num_;
  size_t base_lg_;
};

}  // namespace lsm
