#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include "../sstable/block.h"
#include "../sstable/table.h"
#include "../util/slice.h"
#include "../util/status.h"

namespace lsm {

class Version;
class VersionSet;
class TableCache;

// FileMetaData：单个SSTable文件的元信息
struct FileMetaData {
  FileMetaData() : refs(0), file_size(0), number(0), smallest_key(), largest_key() {}

  int refs;
  uint64_t file_size;
  uint64_t number;  // 文件编号（用于生成文件名）
  Slice smallest_key;
  Slice largest_key;

  // 持久化存储的key副本，避免指针失效
  std::string smallest_key_storage;
  std::string largest_key_storage;

  // 引用计数管理
  void Ref() { refs++; }
  void Unref() {
    refs--;
    assert(refs >= 0);
  }
};

// VersionEdit：记录一次层变更（添加/删除文件）
struct VersionEdit {
  VersionEdit() : comparator_name("leveldb.BytewiseComparator"), log_number(0), prev_log_number(0), next_file_number(0), last_sequence(0) {}

  // 元数据
  std::string comparator_name;
  uint64_t log_number;
  uint64_t prev_log_number;
  uint64_t next_file_number;
  uint64_t last_sequence;

  // 被删除的文件集合
  struct DeletedFile {
    int level;
    uint64_t number;
  };
  std::vector<DeletedFile> deleted_files;

  // 新增的文件集合
  struct NewFile {
    int level;
    std::shared_ptr<FileMetaData> file;
  };
  std::vector<NewFile> new_files;

  void Clear() {
    comparator_name = "leveldb.BytewiseComparator";
    log_number = 0;
    prev_log_number = 0;
    next_file_number = 0;
    last_sequence = 0;
    deleted_files.clear();
    new_files.clear();
  }

  void SetComparatorName(const std::string& name) {
    comparator_name = name;
  }

  void SetLogNumber(uint64_t num) { log_number = num; }
  void SetPrevLogNumber(uint64_t num) { prev_log_number = num; }
  void SetNextFileNumber(uint64_t num) { next_file_number = num; }
  void SetLastSequence(uint64_t seq) { last_sequence = seq; }

  void AddFile(int level, uint64_t file_number, uint64_t file_size,
              const Slice& smallest_key, const Slice& largest_key) {
    NewFile f;
    f.level = level;
    f.file = std::make_shared<FileMetaData>();
    f.file->number = file_number;
    f.file->file_size = file_size;
    f.file->smallest_key_storage = smallest_key.ToString();
    f.file->largest_key_storage = largest_key.ToString();
    f.file->smallest_key = Slice(f.file->smallest_key_storage);
    f.file->largest_key = Slice(f.file->largest_key_storage);
    new_files.push_back(f);
  }

  void DeleteFile(int level, uint64_t file_number) {
    DeletedFile f;
    f.level = level;
    f.number = file_number;
    deleted_files.push_back(f);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);
};

// Version：某一时刻的全量文件视图（Immutable）
class Version {
 public:
  friend class VersionSet;

  explicit Version(VersionSet* vset);
  ~Version();

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  // 引用计数
  void Ref() { refs_++; }
  void Unref() {
    refs_--;
    assert(refs_ >= 0);
    if (refs_ == 0) {
      delete this;
    }
  }

  // 查找key
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

  // 迭代所有文件
  class Iterator;
  Iterator* NewIterator(const ReadOptions& options);

  // VersionSet所属
  VersionSet* const vset_;

  // 双向链表指针（VersionSet中维护）
  Version* prev_;
  Version* next_;

  // 各层文件列表
  static const int kNumLevels = 7;
  std::vector<std::shared_ptr<FileMetaData>> files_[kNumLevels];

 private:
  int refs_;
};

// TableCache：缓存打开的Table对象，避免重复打开文件
class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  TableCache(const TableCache&) = delete;
  TableCache& operator=(const TableCache&) = delete;

  // 返回指定文件编号的Table，可能来自缓存
  Status Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
             const Slice& key, std::string* value);

  // 查找并返回Table，缓存管理
  Status FindTable(uint64_t file_number, uint64_t file_size,
                  std::unique_ptr<Table>* table);

  // 驱逐缓存
  void Evict(uint64_t file_number);

 private:
  struct LRUHandle;
  LRUHandle* lru_;
  std::mutex mutex_;

  const std::string dbname_;
  const Options options_;
};

// VersionSet：管理所有Version、持久化到Manifest
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options& options, TableCache* table_cache);
  ~VersionSet();

  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  // 恢复
  Status Recover(bool* save_manifest);

  // 写MANIFEST
  Status LogAndApply(VersionEdit* edit);

  // 标记文件使用，不被删除
  void AddLiveFiles(std::set<uint64_t>* live);

  // 生成新文件编号
  uint64_t NewFileNumber() { return next_file_number_++; }

  // 日志/序列号管理
  uint64_t GetLogNumber() const { return log_number_; }
  uint64_t GetPrevLogNumber() const { return prev_log_number_; }
  uint64_t GetLastSequence() const { return last_sequence_; }
  void SetLastSequence(uint64_t seq) { last_sequence_ = seq; }

  // 版本切换
  Version* current() const { return current_; }

  // Manifest相关
  std::string DescriptorFileName(uint64_t number);
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // 元信息
  const std::string dbname_;
  const Options options_;
  TableCache* const table_cache_;

 private:
  std::string LogFileName(uint64_t number);
  Status WriteSnapshot(std::fstream* file);
  void AppendVersion(Version* v);

  mutable std::mutex mutex_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;

  std::fstream* descriptor_log_;
  Version dummy_versions_;  // 哨兵节点，链表头
  Version* current_;        // 最新版本
};

}  // namespace lsm
