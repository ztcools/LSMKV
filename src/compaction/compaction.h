#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>
#include "../util/slice.h"
#include "../util/status.h"
#include "../sstable/block.h"
#include "../sstable/table_builder.h"
#include "../version/version.h"
#include "merge_iterator.h"

namespace lsm {

// 前向声明
class VersionSet;

// Compaction类型
enum CompactionType {
  kMinorCompaction,  // MemTable -> L0
  kMajorCompaction   // 上层 -> 下层合并
};

// 一次Compaction的输入和输出
class Compaction {
 public:
  Compaction(int level, const Options& options);
  ~Compaction();

  // 添加输入文件
  void AddInputFile(int level, std::shared_ptr<FileMetaData> file);

  // 输入文件访问
  int level() const { return level_; }
  const std::vector<std::shared_ptr<FileMetaData>>& inputs(int level) const {
    return inputs_[level];
  }
  int num_input_files() const;

  // 是否已经有输入
  bool HasInputs() const;

  // 输出文件（构建过程中添加）
  void AddOutput(std::shared_ptr<FileMetaData> file);
  const std::vector<std::shared_ptr<FileMetaData>> outputs() const { return outputs_; }

  // 临时的版本编辑
  VersionEdit* edit() { return &edit_; }

  // 选项
  const Options& options() const { return options_; }

 private:
  int level_;
  const Options options_;
  VersionEdit edit_;
  std::vector<std::shared_ptr<FileMetaData>> inputs_[Version::kNumLevels];
  std::vector<std::shared_ptr<FileMetaData>> outputs_;
};

// Compaction构建器
class CompactionBuilder {
 public:
  CompactionBuilder(const Options& options, VersionSet* versions);
  ~CompactionBuilder();

  // 调度Compaction：返回需要做的Compaction
  Status PickCompaction(std::unique_ptr<Compaction>* compaction);

  // 执行Compaction
  Status RunCompaction(Compaction* c, const std::string& dbname,
                     TableCache* table_cache);

 private:
  const Options options_;
  VersionSet* versions_;

  // 内部辅助函数
  Status InstallCompactionResults(Compaction* c, VersionSet* vset);
  Status CompactMemTable(std::unique_ptr<Compaction>* compaction);
  Status CompactToLevel(std::unique_ptr<Compaction>* compaction);
  std::shared_ptr<FileMetaData> BuildTable(
      const std::string& dbname, uint64_t file_number, Iterator* iter,
      uint64_t* file_size, std::unique_ptr<TableBuilder>* builder);
};

// 垃圾回收器
class FileDeleter {
 public:
  FileDeleter(const std::string& dbname);
  ~FileDeleter();

  void DeleteFile(uint64_t file_number);
  void MarkFileNumber(uint64_t file_number);
  void DeleteObsoleteFiles(const std::set<uint64_t>& live);

 private:
  const std::string dbname_;
  std::set<uint64_t> pending_deletes_;
};

}  // namespace lsm
