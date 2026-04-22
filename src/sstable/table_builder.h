#pragma once

#include <string>
#include <fstream>
#include <memory>
#include "../util/slice.h"
#include "../util/status.h"
#include "block.h"

namespace lsm {

class TableBuilder {
 public:
  TableBuilder(const Options& options, const std::string& filename);
  ~TableBuilder();

  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  void Add(const Slice& key, const Slice& value);
  void Flush();
  Status Finish();
  void Abandon();

  uint64_t NumEntries() const;
  uint64_t FileSize() const;
  Status status() const;

 private:
  Status WriteBlock(BlockBuilder* block, BlockHandle* handle);
  Status WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  const Options options_;
  std::fstream file_;
  uint64_t offset_;
  bool closed_;

  BlockBuilder data_block_;
  BlockBuilder index_block_;
  std::string last_key_;
  uint64_t num_entries_;

  FilterBlockBuilder filter_block_;

  std::string compressed_output_;
  Status status_;
};

}  // namespace lsm
