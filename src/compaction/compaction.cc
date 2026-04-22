#include "compaction.h"
#include "merge_iterator.h"
#include <cstdio>
#include <cstdint>
#include <memory>

namespace lsm {

// 层的目标大小：L1是10MB，每层乘10
static const uint64_t kLevelTargetSizes[Version::kNumLevels] = {
    0,  // L0 不按大小
    10 * 1024 * 1024,  // L1: 10MB
    100 * 1024 * 1024,  // L2: 100MB
    1024 * 1024 * 1024,  // L3: 1GB
    10 * 1024ULL * 1024ULL * 1024ULL,  // L4: 10GB
    100 * 1024ULL * 1024ULL * 1024ULL,  // L5: 100GB
    1024ULL * 1024ULL * 1024ULL * 1024ULL  // L6: 1TB
};

// ========== Compaction ==========
Compaction::Compaction(int level, const Options& options)
    : level_(level), options_(options) {
}

Compaction::~Compaction() = default;

void Compaction::AddInputFile(int level, std::shared_ptr<FileMetaData> file) {
  assert(level >= 0 && level < Version::kNumLevels);
  inputs_[level].push_back(std::move(file));
}

int Compaction::num_input_files() const {
  int n = 0;
  for (int level = 0; level < Version::kNumLevels; level++) {
    n += inputs_[level].size();
  }
  return n;
}

bool Compaction::HasInputs() const {
  return num_input_files() > 0;
}

void Compaction::AddOutput(std::shared_ptr<FileMetaData> file) {
  outputs_.push_back(std::move(file));
}

// ========== CompactionBuilder ==========
CompactionBuilder::CompactionBuilder(const Options& options, VersionSet* versions)
    : options_(options), versions_(versions) {
}

CompactionBuilder::~CompactionBuilder() = default;

Status CompactionBuilder::PickCompaction(std::unique_ptr<Compaction>* compaction) {
  // 简化：先检查是否有需要做的Major Compaction
  Version* current = versions_->current();

  // 优先检查L0，因为L0文件重叠
  if (!current->files_[0].empty()) {
    // L0有文件，触发Major Compaction
    auto c = std::make_unique<Compaction>(0, options_);
    for (const auto& file : current->files_[0]) {
      c->AddInputFile(0, file);
    }
    *compaction = std::move(c);
    return Status::OK();
  }

  // 检查其他层是否超过大小
  for (int level = 1; level < Version::kNumLevels; level++) {
    uint64_t total_size = 0;
    for (const auto& file : current->files_[level]) {
      total_size += file->file_size;
    }

    if (total_size > kLevelTargetSizes[level]) {
      // 超过大小，选该层的一个文件
      auto c = std::make_unique<Compaction>(level, options_);
      if (!current->files_[level].empty()) {
        // 选第一个文件（简化选择策略）
        c->AddInputFile(level, current->files_[level][0]);
        *compaction = std::move(c);
        return Status::OK();
      }
    }
  }

  return Status::OK();  // 没有需要Compaction的
}

Status CompactionBuilder::RunCompaction(Compaction* c, const std::string& dbname,
                                         TableCache* table_cache) {
  // 1. 收集输入文件的迭代器
  std::vector<std::unique_ptr<Iterator>> iters;
  std::vector<Iterator*> child_iters;

  for (int level = 0; level < Version::kNumLevels; level++) {
    for (const auto& file : c->inputs(level)) {
      std::shared_ptr<Table> table;
      Status s = table_cache->FindTable(file->number, file->file_size, &table);
      if (!s.ok()) {
        return s;
      }
      auto iter = table->NewIterator(ReadOptions{});
      Iterator* raw_iter = iter.get();
      iters.push_back(std::move(iter));
      child_iters.push_back(raw_iter);
    }
  }

  if (child_iters.empty()) {
    // 没有输入文件
    return Status::OK();
  }

  // 2. 使用MergeIterator进行k-way merge
  MergeIterator merge_iter(child_iters);
  merge_iter.SeekToFirst();

  if (!merge_iter.Valid()) {
    // 没有数据
    return Status::OK();
  }

  // 3. 构建输出SSTable文件
  uint64_t file_number = versions_->NewFileNumber();
  std::unique_ptr<TableBuilder> builder;
  std::string last_key;
  std::shared_ptr<FileMetaData> output_file;

  // 实际构建表
  auto last_file = BuildTable(dbname, file_number, &merge_iter, nullptr, &builder);
  if (last_file) {
    c->AddOutput(last_file);
  }

  // 4. 更新edit
  // 标记要删除的旧文件
  for (int level = 0; level < Version::kNumLevels; level++) {
    for (const auto& file : c->inputs(level)) {
      c->edit()->DeleteFile(level, file->number);
    }
  }

  // 标记要添加的新文件
  for (const auto& file : c->outputs()) {
    int output_level = c->level() + 1;
    if (output_level >= Version::kNumLevels) {
      output_level = Version::kNumLevels - 1;  // 最后一层
    }
    c->edit()->AddFile(output_level, file->number, file->file_size,
                       file->smallest_key, file->largest_key);
  }

  // 5. 提交结果
  return InstallCompactionResults(c, versions_);
}

std::shared_ptr<FileMetaData> CompactionBuilder::BuildTable(
    const std::string& dbname, uint64_t file_number, Iterator* iter,
    uint64_t* /* file_size */, std::unique_ptr<TableBuilder>* builder_ptr) {
  std::string filename = dbname + "/" + std::to_string(file_number) + ".sst";

  auto builder = std::make_unique<TableBuilder>(options_, filename);
  Slice last_key;
  Slice first_key;
  bool first = true;

  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    Slice value = iter->value();

    if (first) {
      first = false;
      first_key = key;
      last_key = key;
    }

    // 墓碑清理：如果value是特殊标记或者是旧版本（简化：只保留最新的）
    // 这里简化为：直接添加到builder，忽略重复的
    // 实际实现中应该处理sequence number和删除标记

    builder->Add(key, value);
    last_key = key;
  }

  Status s = builder->Finish();
  if (!s.ok()) {
    // 失败处理
    return nullptr;
  }

  if (builder_ptr) {
    *builder_ptr = std::move(builder);
  }

  // 创建FileMetaData
  auto file_meta = std::make_shared<FileMetaData>();
  file_meta->number = file_number;
  file_meta->file_size = builder->FileSize();
  file_meta->smallest_key_storage = first_key.ToString();
  file_meta->largest_key_storage = last_key.ToString();
  file_meta->smallest_key = Slice(file_meta->smallest_key_storage);
  file_meta->largest_key = Slice(file_meta->largest_key_storage);

  return file_meta;
}

Status CompactionBuilder::InstallCompactionResults(Compaction* c, VersionSet* vset) {
  return vset->LogAndApply(c->edit());
}

// ========== FileDeleter ==========
FileDeleter::FileDeleter(const std::string& dbname)
    : dbname_(dbname) {
}

FileDeleter::~FileDeleter() = default;

void FileDeleter::DeleteFile(uint64_t file_number) {
  pending_deletes_.insert(file_number);
}

void FileDeleter::MarkFileNumber(uint64_t file_number) {
  pending_deletes_.insert(file_number);
}

void FileDeleter::DeleteObsoleteFiles(const std::set<uint64_t>& live) {
  for (const auto file_num : pending_deletes_) {
    if (live.find(file_num) == live.end()) {
      // 不在live中，删除
      std::string filename = dbname_ + "/" + std::to_string(file_num) + ".sst";
      std::remove(filename.c_str());
    }
  }
  pending_deletes_.clear();
}

}  // namespace lsm
