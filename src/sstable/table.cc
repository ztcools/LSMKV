#include "table.h"
#include <cstring>
#include <algorithm>
#include "../util/util.h"

namespace lsm {

// ========== BlockCache LRU 实现 ==========
struct BlockCache::LRUElement {
  std::string key;
  std::unique_ptr<Block> block;
  LRUElement* prev;
  LRUElement* next;

  LRUElement() : prev(nullptr), next(nullptr) {}
};

BlockCache::BlockCache(size_t capacity)
    : head_(new LRUElement()), tail_(new LRUElement()),
      capacity_(capacity), usage_(0) {
  head_->next = tail_;
  tail_->prev = head_;
}

BlockCache::~BlockCache() {
  Clear();
  delete head_;
  delete tail_;
}

void BlockCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& entry : cache_) {
    LRUElement* e = entry.second.get();
    usage_ -= e->block->size();
  }
  cache_.clear();
  head_->next = tail_;
  tail_->prev = head_;
}

void BlockCache::Insert(const std::string& key, std::unique_ptr<Block> block) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 如果已经存在，先删除旧的
  auto it = cache_.find(key);
  if (it != cache_.end()) {
    LRUElement* e = it->second.get();
    usage_ -= e->block->size();
    e->prev->next = e->next;
    e->next->prev = e->prev;
    cache_.erase(it);
  }

  // 创建新 entry
  auto elem = std::make_unique<LRUElement>();
  elem->key = key;
  elem->block = std::move(block);

  // 如果超过 capacity，驱逐最久未使用的
  while (usage_ + elem->block->size() > capacity_ && cache_.size() > 0) {
    LRUElement* to_remove = tail_->prev;
    usage_ -= to_remove->block->size();
    to_remove->prev->next = tail_;
    tail_->prev = to_remove->prev;
    cache_.erase(to_remove->key);
  }

  // 添加到 LRU 头部
  usage_ += elem->block->size();
  elem->next = head_->next;
  elem->prev = head_;
  head_->next->prev = elem.get();
  head_->next = elem.get();
  cache_[key] = std::move(elem);
}

Block* BlockCache::Lookup(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_.find(key);
  if (it == cache_.end()) {
    return nullptr;
  }
  // 移动到头部
  LRUElement* e = it->second.get();
  e->prev->next = e->next;
  e->next->prev = e->prev;
  e->prev = head_;
  e->next = head_->next;
  head_->next->prev = e;
  head_->next = e;
  return e->block.get();
}

void BlockCache::Erase(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_.find(key);
  if (it != cache_.end()) {
    LRUElement* e = it->second.get();
    usage_ -= e->block->size();
    e->prev->next = e->next;
    e->next->prev = e->prev;
    cache_.erase(it);
  }
}

// ========== Table::Open ==========
Status Table::Open(const Options& options, const std::string& filename,
                   std::unique_ptr<Table>* table) {
  std::fstream file(filename, std::ios::in | std::ios::binary | std::ios::ate);
  if (!file) {
    return Status::IOError("open sstable failed");
  }

  uint64_t file_size = file.tellg();
  if (file_size < Footer::kEncodedLength) {
    return Status::Corruption("sstable is too small");
  }

  // 读取 Footer
  std::vector<char> footer_space(Footer::kEncodedLength);
  file.seekg(file_size - Footer::kEncodedLength);
  file.read(footer_space.data(), Footer::kEncodedLength);
  if (!file) {
    return Status::IOError("read sstable footer failed");
  }

  Slice footer_input(footer_space.data(), Footer::kEncodedLength);
  Footer footer;
  Status s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) {
    return s;
  }

  // 读取 Index Block
  std::unique_ptr<Block> index_block;
  {
    Slice contents;
    CompressionType type;
    std::unique_ptr<Table> tmp_table(new Table(
        options, filename, std::unique_ptr<FilterBlockReader>(),
        std::unique_ptr<Block>(), std::unique_ptr<BlockCache>()));
    tmp_table->file_ = std::move(file);
    tmp_table->file_size_ = file_size;
    s = tmp_table->ReadRawBlock(footer.index_handle, &contents, &type);
    if (s.ok()) {
      index_block = std::make_unique<Block>(contents.data(), contents.size(),
                                            type != kNoCompression, type);
    }
    file = std::move(tmp_table->file_);
  }
  if (!s.ok()) {
    return s;
  }

  // 读取 Meta Index Block
  std::unique_ptr<FilterBlockReader> filter;
  {
    Slice contents;
    CompressionType type;
    std::unique_ptr<Table> tmp_table(new Table(
        options, filename, std::unique_ptr<FilterBlockReader>(),
        std::move(index_block), std::unique_ptr<BlockCache>()));
    tmp_table->file_ = std::move(file);
    tmp_table->file_size_ = file_size;
    s = tmp_table->ReadRawBlock(footer.metaindex_handle, &contents, &type);
    if (s.ok()) {
      Block meta_block(contents.data(), contents.size(), type != kNoCompression, type);
      Block::Iterator iter(&meta_block);
      for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        Slice key = iter.key();
        Slice value = iter.value();
        if (key.starts_with("filter.")) {
          BlockHandle handle;
          Slice input = value;
          if (handle.DecodeFrom(&input).ok()) {
            Slice filter_contents;
            CompressionType filter_type;
            s = tmp_table->ReadRawBlock(handle, &filter_contents, &filter_type);
            if (s.ok()) {
              filter = std::make_unique<FilterBlockReader>(filter_contents);
            }
          }
        }
      }
    }
    index_block = std::move(tmp_table->index_block_);
    file = std::move(tmp_table->file_);
  }

  if (!s.ok()) {
    return s;
  }

  auto cache = std::make_unique<BlockCache>(8 << 20);  // 默认 8MB 缓存

  table->reset(new Table(options, filename, std::move(filter),
                         std::move(index_block), std::move(cache)));
  (*table)->file_ = std::move(file);
  (*table)->file_size_ = file_size;

  return Status::OK();
}

Table::Table(const Options& options, const std::string& filename,
             std::unique_ptr<FilterBlockReader> filter,
             std::unique_ptr<Block> index_block,
             std::unique_ptr<BlockCache> cache)
    : options_(options),
      filename_(filename),
      filter_(std::move(filter)),
      index_block_(std::move(index_block)),
      cache_(std::move(cache)),
      file_size_(0) {}

Table::~Table() = default;

Status Table::ReadRawBlock(const BlockHandle& handle, Slice* content,
                          CompressionType* type) const {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t n = handle.size + 5;  // 5 bytes trailer
  std::vector<char> buf(n);

  file_.seekg(handle.offset);
  file_.read(buf.data(), n);
  if (!file_) {
    return Status::IOError("read sstable block failed");
  }

  const char* data = buf.data();
  *type = static_cast<CompressionType>(data[handle.size]);
  // Verify checksum (optional)
  uint32_t expected_crc = util::DecodeFixed32(data + handle.size + 1);
  uint32_t actual_crc = util::Crc32(data, handle.size);
  actual_crc = util::Crc32Extend(actual_crc, data + handle.size, 1);

  if (expected_crc != actual_crc) {
    // TODO: 实际使用需要检查，这里先跳过
  }

  *content = Slice(data, handle.size);
  return Status::OK();
}

Status Table::ReadBlock(const ReadOptions& options, const BlockHandle& handle,
                       std::unique_ptr<Block>* block) const {
  std::string cache_key = filename_ + "/" + std::to_string(handle.offset);
  Block* cached = cache_->Lookup(cache_key);
  if (cached != nullptr) {
    // TODO: 返回副本或正确的生命周期管理
    // 这里简化处理
    *block = std::make_unique<Block>(cached->data(), cached->size());
    return Status::OK();
  }

  Slice contents;
  CompressionType type;
  Status s = ReadRawBlock(handle, &contents, &type);
  if (!s.ok()) {
    return s;
  }

  auto b = std::make_unique<Block>(contents.data(), contents.size(),
                                    type != kNoCompression, type);
  *block = std::move(b);

  if (options.fill_cache) {
    // 缓存块副本
    cache_->Insert(cache_key, std::make_unique<Block>(contents.data(), contents.size()));
  }

  return Status::OK();
}

Status Table::Get(const ReadOptions& options, const Slice& key,
                  std::string* value) {
  Block::Iterator iiter(index_block_.get());
  iiter.Seek(key);
  if (!iiter.Valid()) {
    return Status::NotFound("");
  }

  Slice handle_value = iiter.value();
  BlockHandle handle;
  Slice input = handle_value;
  Status s = handle.DecodeFrom(&input);
  if (!s.ok()) {
    return s;
  }

  // 先检查 BloomFilter
  if (filter_ && !filter_->KeyMayMatch(handle.offset, key)) {
    return Status::NotFound("");
  }

  std::unique_ptr<Block> block;
  s = ReadBlock(options, handle, &block);
  if (!s.ok()) {
    return s;
  }

  Block::Iterator iter(block.get());
  iter.Seek(key);
  if (iter.Valid() && iter.key().compare(key) == 0) {
    *value = std::string(iter.value().data(), iter.value().size());
    return Status::OK();
  } else {
    return Status::NotFound("");
  }
}

bool Table::KeyMayMatch(const Slice& key) {
  if (filter_ == nullptr) {
    return true;
  }

  Block::Iterator iiter(index_block_.get());
  iiter.Seek(key);
  if (!iiter.Valid()) {
    return true;
  }

  Slice handle_value = iiter.value();
  BlockHandle handle;
  Slice input = handle_value;
  if (!handle.DecodeFrom(&input).ok()) {
    return true;
  }
  return filter_->KeyMayMatch(handle.offset, key);
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) {
  Block::Iterator iiter(index_block_.get());
  iiter.Seek(key);
  if (iiter.Valid()) {
    Slice handle_value = iiter.value();
    BlockHandle handle;
    Slice input = handle_value;
    if (handle.DecodeFrom(&input).ok()) {
      return handle.offset;
    }
  }
  return file_size_;
}

// ========== Table::Iterator ==========
Table::Iterator::Iterator(const Table* table, const ReadOptions& options)
    : table_(table), options_(options),
      index_iter_(std::make_unique<Block::Iterator>(table->index_block_.get())),
      data_iter_(nullptr) {}

Table::Iterator::~Iterator() = default;

bool Table::Iterator::Valid() const {
  return data_iter_ != nullptr && data_iter_->Valid();
}

Slice Table::Iterator::key() const {
  assert(Valid());
  return data_iter_->key();
}

Slice Table::Iterator::value() const {
  assert(Valid());
  return data_iter_->value();
}

Status Table::Iterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  if (data_iter_) {
    return data_iter_->status();
  }
  return index_iter_->status();
}

void Table::Iterator::SeekToFirst() {
  index_iter_->SeekToFirst();
  SetDataBlock();
}

void Table::Iterator::SeekToLast() {
  index_iter_->SeekToLast();
  SetDataBlock();
  if (data_iter_) {
    data_iter_->SeekToLast();
  }
}

void Table::Iterator::Seek(const Slice& target) {
  index_iter_->Seek(target);
  SetDataBlock();
  if (data_iter_) {
    data_iter_->Seek(target);
  }
}

void Table::Iterator::SetDataBlock() {
  if (!index_iter_->Valid()) {
    data_iter_.reset();
    return;
  }

  Slice handle_value = index_iter_->value();
  BlockHandle handle;
  Slice input = handle_value;
  Status s = handle.DecodeFrom(&input);
  if (!s.ok()) {
    status_ = s;
    data_iter_.reset();
    return;
  }

  std::unique_ptr<Block> block;
  s = table_->ReadBlock(options_, handle, &block);
  if (!s.ok()) {
    status_ = s;
    data_iter_.reset();
    return;
  }

  data_block_key_ = std::string(index_iter_->key().data(), index_iter_->key().size());
  data_iter_ = std::make_unique<Block::Iterator>(block.get());
}

void Table::Iterator::Next() {
  assert(Valid());
  data_iter_->Next();
  if (!data_iter_->Valid()) {
    NextIndex();
  }
}

void Table::Iterator::NextIndex() {
  index_iter_->Next();
  SetDataBlock();
  if (data_iter_) {
    data_iter_->SeekToFirst();
  }
}

void Table::Iterator::Prev() {
  assert(Valid());
  data_iter_->Prev();
  if (!data_iter_->Valid()) {
    // 回到上一个 index
    index_iter_->Prev();
    SetDataBlock();
    if (data_iter_) {
      data_iter_->SeekToLast();
    }
  }
}

Table::Iterator* Table::NewIterator(const ReadOptions& options) {
  return new Iterator(this, options);
}

}  // namespace lsm
