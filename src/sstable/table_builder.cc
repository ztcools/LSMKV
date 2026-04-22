#include "table_builder.h"
#include <cstring>
#include "../util/util.h"

namespace lsm {

TableBuilder::TableBuilder(const Options& options, const std::string& filename)
    : options_(options),
      file_(filename, std::ios::out | std::ios::binary | std::ios::trunc),
      offset_(0),
      closed_(false),
      data_block_(options.block_restart_interval),
      num_entries_(0),
      status_(Status::OK()) {
}

TableBuilder::~TableBuilder() {
  if (!closed_) {
    // 如果没有关闭，先 Abandon
    Abandon();
  }
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  if (!status_.ok()) return;
  if (num_entries_ > 0) {
    assert(key.compare(Slice(last_key_)) > 0);
  }

  // 如果 Block 超过阈值，先 flush
  if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
    Flush();
  }

  if (data_block_.empty()) {
    filter_block_.StartBlock(offset_);
  }
  data_block_.Add(key, value);
  filter_block_.AddKey(key);
  last_key_.assign(key.data(), key.size());
  num_entries_++;
}

void TableBuilder::Flush() {
  if (!status_.ok()) return;
  if (data_block_.empty()) return;

  BlockHandle handle;
  status_ = WriteBlock(&data_block_, &handle);
  if (status_.ok()) {
    // 将这个 data block 的 handle 添加到 index block
    std::string handle_encoding;
    handle.EncodeTo(&handle_encoding);
    index_block_.Add(last_key_, handle_encoding);
    data_block_.Reset();
  }
}

Status TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  Slice raw = block->Finish();
  return WriteRawBlock(raw, options_.compression, handle);
}

Status TableBuilder::WriteRawBlock(const Slice& raw, CompressionType compression,
                                  BlockHandle* handle) {
  // 压缩（如果需要）
  Slice contents = raw;
  // TODO: 实际压缩代码
  compression = kNoCompression;

  // 计算 CRC
  uint32_t crc = util::Crc32(contents.data(), contents.size());
  crc = util::Crc32Extend(crc, reinterpret_cast<const char*>(&compression), 1);

  // 写入内容
  file_.write(contents.data(), contents.size());
  if (!file_) {
    return Status::IOError("write sstable block failed");
  }

  // 写入 trailer
  char trailer[5];
  trailer[0] = static_cast<char>(compression);
  util::EncodeFixed32(trailer + 1, crc);
  file_.write(trailer, 5);

  handle->offset = offset_;
  handle->size = contents.size();
  offset_ += contents.size() + 5;
  return Status::OK();
}

Status TableBuilder::Finish() {
  if (!status_.ok()) return status_;

  // 1. 写入最后一个 Data Block
  Flush();
  if (!status_.ok()) return status_;

  // 2. 写入 Filter Block
  BlockHandle filter_handle;
  if (status_.ok()) {
    Slice filter = filter_block_.Finish();
    status_ = WriteRawBlock(filter, kNoCompression, &filter_handle);
  }

  // 3. 写入 Meta Index Block
  BlockHandle metaindex_handle;
  if (status_.ok()) {
    BlockBuilder meta_index_block;
    std::string key = "filter.";
    key += "bloomfilter";  // 过滤器名称
    std::string handle_encoding;
    filter_handle.EncodeTo(&handle_encoding);
    meta_index_block.Add(key, handle_encoding);
    status_ = WriteBlock(&meta_index_block, &metaindex_handle);
  }

  // 4. 写入 Index Block
  BlockHandle index_handle;
  if (status_.ok()) {
    status_ = WriteBlock(&index_block_, &index_handle);
  }

  // 5. 写入 Footer
  if (status_.ok()) {
    Footer footer;
    footer.metaindex_handle = metaindex_handle;
    footer.index_handle = index_handle;
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    file_.write(footer_encoding.data(), footer_encoding.size());
    if (!file_) {
      return Status::IOError("write sstable footer failed");
    }
    offset_ += footer_encoding.size();
  }

  closed_ = true;
  return status_;
}

void TableBuilder::Abandon() {
  closed_ = true;
}

uint64_t TableBuilder::NumEntries() const {
  return num_entries_;
}

uint64_t TableBuilder::FileSize() const {
  return offset_;
}

Status TableBuilder::status() const {
  return status_;
}

}  // namespace lsm
