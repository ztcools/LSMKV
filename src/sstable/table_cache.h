#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <mutex>
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/cache.h"
#include "table.h"

namespace lsm {

class TableCache {
 public:
  TableCache(const Options& options, const std::string& dbname, int entries = 1000);

  ~TableCache();

  Status Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
             const Slice& key, std::string* value);

  Status NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
                     std::unique_ptr<Iterator>* iter);

  Status Evict(uint64_t file_number);

  Status FindTable(uint64_t file_number, uint64_t file_size, std::shared_ptr<Table>* table);

  const Options options_;
  const std::string dbname_;
  std::shared_ptr<Cache> cache_;
  mutable std::mutex mutex_;
};

}  // namespace lsm
