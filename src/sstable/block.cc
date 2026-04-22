#include "block.h"
#include <algorithm>
#include <cstring>
#include "../util/util.h"

namespace lsm {

// ========== BlockHandle ==========

void BlockHandle::EncodeTo(std::string* dst) const {
  util::PutVarint64(dst, offset);
  util::PutVarint64(dst, size);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (util::GetVarint64(input, &offset) && util::GetVarint64(input, &size)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

// ========== Footer ==========
void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  metaindex_handle.EncodeTo(dst);
  index_handle.EncodeTo(dst);
  dst->resize(original_size + kEncodedLength - 8);
  util::PutFixed64(dst, kTableMagicNumber);
}

Status Footer::DecodeFrom(Slice* input) {
  if (input->size() < kEncodedLength) {
    return Status::Corruption("not enough bytes for footer");
  }
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  uint64_t magic = util::DecodeFixed64(magic_ptr);
  if (magic != kTableMagicNumber) {
    return Status::Corruption("bad magic number");
  }
  Slice footer_input(input->data(), kEncodedLength - 8);
  Status s = metaindex_handle.DecodeFrom(&footer_input);
  if (s.ok()) {
    s = index_handle.DecodeFrom(&footer_input);
  }
  return s;
}

// ========== Block ==========
Block::Block(const char* data, size_t size, bool compressed, CompressionType type)
    : data_(data), size_(size), compressed_(compressed), compression_type_(type),
      decompressed_(nullptr), decompressed_size_(0) {
  // 解压缩（如果需要）
  if (compressed_) {
    // TODO: 实际的解压缩代码
    // 这里先假设没有压缩或者跳过解压缩，以便可以编译通过
  }
}

Block::~Block() {
  if (decompressed_) {
    delete[] decompressed_;
  }
}

uint32_t Block::NumRestarts() const {
  const char* p = decompressed_ ? decompressed_ : data_;
  size_t s = decompressed_ ? decompressed_size_ : size_;
  assert(s >= 4);
  return util::DecodeFixed32(p + s - 4);
}

const char* Block::RestartPoint(uint32_t index) const {
  const char* p = decompressed_ ? decompressed_ : data_;
  uint32_t num_restarts = NumRestarts();
  assert(index < num_restarts);
  return p + util::DecodeFixed32(p + (num_restarts - index - 1) * 4 + size_ - 4 * (num_restarts + 1));
}

// ========== Block::Iterator ==========
Block::Iterator::Iterator(const Block* block)
    : block_(block), data_end_(block->decompressed_ ? block->decompressed_ + block->decompressed_size_ : block->data_ + block->size_),
      current_(nullptr), restart_index_(0), status_(Status::OK()) {}

bool Block::Iterator::Valid() const {
  return current_ != nullptr;
}

Slice Block::Iterator::key() const {
  assert(Valid());
  return Slice(key_);
}

Slice Block::Iterator::value() const {
  assert(Valid());
  return value_;
}

void Block::Iterator::SeekToFirst() {
  Seek(Slice(""));
}

void Block::Iterator::SeekToLast() {
  uint32_t num_restarts = block_->NumRestarts();
  if (num_restarts == 0) {
    current_ = nullptr;
    return;
  }
  restart_index_ = num_restarts - 1;
  current_ = block_->RestartPoint(restart_index_);
  ParseNextKey();
  while (current_ < data_end_ && ParseNextKey()) {
    // 继续直到最后一个
  }
  current_ = block_->RestartPoint(restart_index_);
  ParseNextKey();
}

void Block::Iterator::Seek(const Slice& target) {
  // 二分查找找到合适的 restart point
  restart_index_ = BinarySearchIndex(target);

  // 从 restart point 开始线性查找
  current_ = block_->RestartPoint(restart_index_);
  if (ParseNextKey()) {
    do {
      if (Slice(key_).compare(target) >= 0) {
        return;
      }
    } while (ParseNextKey());
  }
  current_ = nullptr;
}

uint32_t Block::Iterator::BinarySearchIndex(const Slice& target) {
  uint32_t left = 0;
  uint32_t right = block_->NumRestarts() - 1;

  while (left < right) {
    uint32_t mid = left + (right - left) / 2;
    const char* p = block_->RestartPoint(mid);
    uint32_t shared, non_shared, value_length;
    const char* q = p;
    q = util::DecodeVarint32(q, data_end_, &shared);
    q = util::DecodeVarint32(q, data_end_, &non_shared);
    q = util::DecodeVarint32(q, data_end_, &value_length);
    Slice mid_key(q, non_shared);
    if (mid_key.compare(target) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

void Block::Iterator::Next() {
  assert(Valid());
  ParseNextKey();
}

void Block::Iterator::Prev() {
  assert(Valid());

  // 先回到前一个 restart point
  const char* original = current_;
  while (block_->RestartPoint(restart_index_) >= original) {
    if (restart_index_ == 0) {
      current_ = nullptr;
      restart_index_ = 0;
      return;
    }
    restart_index_--;
  }

  // 然后向前扫描到最后一个小于 current 的
  do {
    current_ = block_->RestartPoint(restart_index_);
    // 从 restart index 开始解析所有的 key
  } while (ParseNextKey() && current_ < original);

  if (!Valid()) {
    return;
  }
  do {
    const char* pos = current_;
    if (!ParseNextKey()) break;
    if (current_ >= original) {
      current_ = pos;
      ParseNextKey();
      return;
    }
  } while (true);
}

bool Block::Iterator::ParseNextKey() {
  const char* p = current_;
  const char* limit = data_end_;

  // 解析 key
  uint32_t shared, non_shared, value_length;
  p = util::DecodeVarint32(p, limit, &shared);
  if (p == nullptr) {
    current_ = nullptr;
    return false;
  }
  p = util::DecodeVarint32(p, limit, &non_shared);
  if (p == nullptr) {
    current_ = nullptr;
    return false;
  }
  p = util::DecodeVarint32(p, limit, &value_length);
  if (p == nullptr) {
    current_ = nullptr;
    return false;
  }

  if (key_.size() < shared) {
    status_ = Status::Corruption("corrupted block entry");
    current_ = nullptr;
    return false;
  }
  key_.resize(shared);
  key_.append(p, non_shared);
  value_ = Slice(p + non_shared, value_length);
  current_ = p + non_shared + value_length;

  // 更新 restart index
  while (restart_index_ + 1 < block_->NumRestarts() &&
         block_->RestartPoint(restart_index_ + 1) < current_) {
    restart_index_++;
  }
  return true;
}

// ========== BlockBuilder ==========
BlockBuilder::BlockBuilder(size_t block_restart_interval)
    : block_restart_interval_(block_restart_interval),
      counter_(0), finished_(false) {
  restarts_.push_back(0);
}

BlockBuilder::~BlockBuilder() = default;

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size() +                     // raw data
         restarts_.size() * sizeof(uint32_t) + // restart array
         sizeof(uint32_t);                      // restarts length
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_slice(last_key_);
  assert(!finished_);
  assert(counter_ <= static_cast<int>(block_restart_interval_));

  size_t shared = 0;
  if (static_cast<size_t>(counter_) < block_restart_interval_) {
    // 计算与前一个 key 的共享前缀长度
    const size_t min_len = std::min(last_key_slice.size(), key.size());
    while (shared < min_len && last_key_slice[shared] == key[shared]) {
      shared++;
    }
  } else {
    // 需要新的 restart point
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }

  const size_t non_shared = key.size() - shared;

  // 编码 entry
  util::PutVarint32(&buffer_, shared);
  util::PutVarint32(&buffer_, non_shared);
  util::PutVarint32(&buffer_, value.size());
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // 更新状态
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  counter_++;
}

Slice BlockBuilder::Finish() {
  // 追加 restart 数组
  for (size_t i = 0; i < restarts_.size(); i++) {
    util::PutFixed32(&buffer_, restarts_[i]);
  }
  util::PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

// ========== FilterBlockBuilder ==========
FilterBlockBuilder::FilterBlockBuilder() = default;
FilterBlockBuilder::~FilterBlockBuilder() = default;

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 为新的 block 准备
  uint64_t filter_index = (block_offset / 2048);  // 每 2KB 一个 filter
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  key_offsets_.push_back(keys_.size());
  keys_.append(key.data(), key.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!key_offsets_.empty()) {
    GenerateFilter();
  }

  // Append filter offsets for each filter
  uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    util::PutFixed32(&result_, filter_offsets_[i]);
  }

  util::PutFixed32(&result_, array_offset);
  result_.push_back(11);  // Base: 2048 (1 << 11)

  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = key_offsets_.size();
  if (num_keys == 0) {
    filter_offsets_.push_back(result_.size());
    return;
  }

  key_offsets_.push_back(keys_.size());

  // TODO: 生成 BloomFilter 位图
  // 这里简化处理，用简单的 hash 代替
  for (size_t i = 0; i < num_keys; ++i) {
    const char* base = keys_.data() + key_offsets_[i];
    size_t len = key_offsets_[i + 1] - key_offsets_[i];
    // 简单存储 key 的 hash 作为占位
    uint32_t h = util::Hash(base, len, 0xbc9f1d34);
    util::PutFixed32(&result_, h);
  }

  filter_offsets_.push_back(result_.size());

  keys_.clear();
  key_offsets_.clear();
}

// ========== FilterBlockReader ==========
FilterBlockReader::FilterBlockReader(const Slice& contents)
    : data_(contents.data()),
      offset_(nullptr),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;
  base_lg_ = contents[n - 1];
  uint32_t last_word = util::DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  offset_ = contents.data() + last_word;
  num_ = (n - 5 - last_word) / 4;
}

FilterBlockReader::~FilterBlockReader() = default;

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  (void)key;
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // TODO: 实际的 BloomFilter 检查
    // 这里简化处理
    return true;
  }
  return true;
}

}  // namespace lsm
