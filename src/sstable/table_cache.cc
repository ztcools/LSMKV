#include "table_cache.h"
#include <cstring>

namespace lsm {

static void DeleteTable(const Slice& /* key */, void* value) {
  Table* t = reinterpret_cast<Table*>(value);
  delete t;
}

TableCache::TableCache(const Options& options, const std::string& dbname, int entries)
    : options_(options), dbname_(dbname) {
  cache_.reset(NewLRUCache(entries));
}

TableCache::~TableCache() = default;

Status TableCache::FindTable(uint64_t file_number, uint64_t /* file_size */,
                            std::shared_ptr<Table>* table) {
  Status s;
  std::string filename = dbname_ + "/" + std::to_string(file_number) + ".sst";

  Cache::Handle* handle = cache_->Lookup(Slice(filename));
  if (handle != nullptr) {
    *table = std::shared_ptr<Table>(
        reinterpret_cast<Table*>(handle->Value()),
        [this, filename, handle](Table* /* t */) {
          cache_->Release(handle);
        });
    return Status::OK();
  }

  std::unique_ptr<Table> t;
  s = Table::Open(options_, filename, &t);
  if (s.ok()) {
    Table* raw_t = t.release();
    Cache::Handle* h = cache_->Insert(Slice(filename), raw_t, 1, &DeleteTable);
    *table = std::shared_ptr<Table>(
        raw_t,
        [this, h, filename](Table* /* t */) {
          cache_->Release(h);
        });
  }
  return s;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                      uint64_t file_size, const Slice& key, std::string* value) {
  std::shared_ptr<Table> t;
  Status s = FindTable(file_number, file_size, &t);
  if (s.ok()) {
    s = t->Get(options, key, value);
  }
  return s;
}

Status TableCache::NewIterator(const ReadOptions& options, uint64_t file_number,
                              uint64_t file_size, std::unique_ptr<Iterator>* iter) {
  std::shared_ptr<Table> t;
  Status s = FindTable(file_number, file_size, &t);
  if (s.ok()) {
    *iter = t->NewIterator(options);
  }
  return s;
}

Status TableCache::Evict(uint64_t file_number) {
  std::string filename = dbname_ + "/" + std::to_string(file_number) + ".sst";
  cache_->Erase(Slice(filename));
  return Status::OK();
}

}  // namespace lsm
