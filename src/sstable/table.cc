#include "table.h"
#include <cstring>
#include "../util/util.h"
#include "../util/cache.h"

namespace lsm {

// ========== Table::Open ==========
Status Table::Open(const Options& options, const std::string& filename,
                   std::unique_ptr<Table>* table) {
  std::fstream file(filename, std::ios::in | std::ios::binary);
  if (!file.is_open()) {
    return Status::IOError("open sstable failed");
  }

  // 获取文件大小
  file.seekg(0, std::ios::end);
  uint64_t file_size = file.tellg();
  if (file_size < Footer::kEncodedLength) {
    return Status::Corruption("file too short for footer");
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
        std::unique_ptr<Block>(), std::shared_ptr<Cache>()));
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
        std::move(index_block), std::shared_ptr<Cache>()));
    tmp_table->file_ = std::move(file);
    tmp_table->file_size_ = file_size;
    s = tmp_table->ReadRawBlock(footer.metaindex_handle, &contents, &type);
    if (s.ok()) {
      Block meta_block(contents.data(), contents.size(), type != kNoCompression, type);
      auto iter = meta_block.NewIterator();
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        Slice value = iter->value();
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

  auto cache = std::shared_ptr<Cache>(NewLRUCache(8 << 20));  // 默认 8MB 缓存

  table->reset(new Table(options, filename, std::move(filter),
                         std::move(index_block), cache));
  (*table)->file_ = std::move(file);
  (*table)->file_size_ = file_size;

  return Status::OK();
}

Table::Table(const Options& options, const std::string& filename,
             std::unique_ptr<FilterBlockReader> filter,
             std::unique_ptr<Block> index_block,
             std::shared_ptr<Cache> cache)
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
  if (!file_.is_open()) {
    return Status::IOError("file not open");
  }

  uint64_t n = handle.size + 5;  // 5 bytes trailer
  std::vector<char> buf(n);
  file_.seekg(handle.offset);
  file_.read(buf.data(), n);
  if (!file_) {
    return Status::IOError("read block failed");
  }

  const char* data = buf.data();
  *type = static_cast<CompressionType>(data[handle.size]);
  uint32_t crc = util::DecodeFixed32(data + handle.size + 1);
  (void)crc;  // 暂时忽略 CRC 检查
  // TODO: 实际使用时需要检查 CRC
  uint32_t actual_crc = util::Crc32(data, handle.size);
  actual_crc = util::Crc32Extend(actual_crc, data + handle.size, 1);
  (void)actual_crc;
  if (crc != actual_crc) {
    // TODO: 实际使用需要检查，这里先跳过
  }

  *content = Slice(data, handle.size);
  return Status::OK();
}

Status Table::ReadBlock(const ReadOptions& /* options */, const BlockHandle& handle,
                       std::unique_ptr<Block>* block) const {
  Slice contents;
  CompressionType type;
  Status s = ReadRawBlock(handle, &contents, &type);
  if (!s.ok()) {
    return s;
  }

  auto b = std::make_unique<Block>(contents.data(), contents.size(),
                                    type != kNoCompression, type);
  *block = std::move(b);

  return Status::OK();
}

Status Table::Get(const ReadOptions& options, const Slice& key,
                  std::string* value) {
  auto iiter = index_block_->NewIterator();
  iiter->Seek(key);
  if (!iiter->Valid()) {
    return Status::NotFound("");
  }

  Slice handle_value = iiter->value();
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

  auto iter = block->NewIterator();
  iter->Seek(key);
  if (iter->Valid() && iter->key().compare(key) == 0) {
    *value = std::string(iter->value().data(), iter->value().size());
    return Status::OK();
  } else {
    return Status::NotFound("");
  }
}

bool Table::KeyMayMatch(const Slice& key) {
  if (filter_ == nullptr) {
    return true;
  }

  auto iiter = index_block_->NewIterator();
  iiter->Seek(key);
  if (!iiter->Valid()) {
    return true;
  }

  Slice handle_value = iiter->value();
  BlockHandle handle;
  Slice input = handle_value;
  if (!handle.DecodeFrom(&input).ok()) {
    return true;
  }
  return filter_->KeyMayMatch(handle.offset, key);
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) {
  auto iiter = index_block_->NewIterator();
  iiter->Seek(key);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    Slice input = handle_value;
    if (handle.DecodeFrom(&input).ok()) {
      return handle.offset;
    }
  }
  return file_size_;
}

// ========== TableIterator ==========
TableIterator::TableIterator(const Table* table, const ReadOptions& options)
    : table_(table), options_(options),
      index_iter_(table->index_block_->NewIterator()),
      data_iter_(nullptr) {}

TableIterator::~TableIterator() = default;

bool TableIterator::Valid() const {
  return data_iter_ != nullptr && data_iter_->Valid();
}

Slice TableIterator::key() const {
  assert(Valid());
  return data_iter_->key();
}

Slice TableIterator::value() const {
  assert(Valid());
  return data_iter_->value();
}

Status TableIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  if (data_iter_) {
    return data_iter_->status();
  }
  return status_;
}

void TableIterator::SeekToFirst() {
  index_iter_->SeekToFirst();
  SetDataBlock();
  if (data_iter_) {
    data_iter_->SeekToFirst();
  }
}

void TableIterator::SeekToLast() {
  index_iter_->SeekToLast();
  SetDataBlock();
  if (data_iter_) {
    data_iter_->SeekToLast();
  }
}

void TableIterator::Seek(const Slice& target) {
  index_iter_->Seek(target);
  SetDataBlock();
  if (data_iter_) {
    data_iter_->Seek(target);
  }
}

void TableIterator::Next() {
  assert(Valid());
  data_iter_->Next();
  if (!data_iter_->Valid()) {
    NextIndex();
  }
}

void TableIterator::Prev() {
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

void TableIterator::SetDataBlock() {
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
  data_iter_ = block->NewIterator();
}

void TableIterator::NextIndex() {
  index_iter_->Next();
  SetDataBlock();
  if (data_iter_) {
    data_iter_->SeekToFirst();
  }
}

std::unique_ptr<Iterator> Table::NewIterator(const ReadOptions& options) {
  return std::make_unique<TableIterator>(this, options);
}

}  // namespace lsm
