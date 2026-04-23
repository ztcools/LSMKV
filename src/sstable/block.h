#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/iterator.h"
#include "../util/options.h"

namespace lsm {

static const uint64_t kTableMagicNumber = 0x57fb088b46db80e8ULL;

// Block句柄（位置 + 大小）
struct BlockHandle {
  static const size_t kMaxEncodedLength = 10 + 10;

  BlockHandle() : offset(0), size(0) {}
  BlockHandle(uint64_t offset, uint64_t size) : offset(offset), size(size) {}

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  uint64_t offset;
  uint64_t size;
};

// 文件尾，包含：
// - Metaindex Block 的 BlockHandle
// - Index Block 的 BlockHandle
struct Footer {
  static const size_t kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  BlockHandle metaindex_handle;
  BlockHandle index_handle;
};

// 数据块
class Block {
 public:
  // 注意：如果 compressed 为 false，Block 不会持有 data 的所有权
  // 如果 compressed 为 true，会复制并解压
  Block(const char* data, size_t size, bool compressed = false,
        CompressionType type = kNoCompression);

  // 支持拷贝（用于缓存）
  Block(const Block& other);
  Block& operator=(const Block& other) = delete;

  ~Block();

  const char* data() const { return data_; }
  size_t size() const { return size_; }

  size_t ApproximateMemoryUsage() const {
    size_t usage = sizeof(*this) + size_;
    if (decompressed_) usage += decompressed_size_;
    if (owned_data_) usage += size_;  // 如果自己持有数据，再加一份
    return usage;
  }

  // 迭代器
  std::unique_ptr<Iterator> NewIterator();

 private:
  friend class BlockIterator;

  uint32_t NumRestarts() const;
  const char* RestartPoint(uint32_t index) const;

  const char* data_;
  size_t size_;
  bool compressed_;
  CompressionType compression_type_;
  char* decompressed_;
  size_t decompressed_size_;
  bool owned_data_;  // 是否自己持有数据的所有权
  char* owned_buf_;  // 自己持有的数据缓冲区
};

// Block迭代器
class BlockIterator : public Iterator {
 public:
  explicit BlockIterator(const Block* block);

  bool Valid() const override;
  Slice key() const override;
  Slice value() const override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  Status status() const override { return status_; }

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
  uint32_t counter_;
  bool finished_;
};

// 过滤器块读写
class FilterBlockReader {
 public:
  FilterBlockReader(const Slice& data);

  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const char* data_;
  size_t size_;
  const char* filter_data_;
  const char* offset_;
  uint32_t num_;
  uint32_t base_lg_;
};

class FilterBlockWriter {
 public:
  FilterBlockWriter() = default;
  ~FilterBlockWriter() = default;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();
  
  std::vector<Slice> keys_;
  std::string result_;
  std::vector<uint32_t> filter_offsets_;
  uint64_t last_block_offset_;
};

}  // namespace lsm
