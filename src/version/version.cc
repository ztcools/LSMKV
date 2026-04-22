#include "version.h"
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include "../util/util.h"

namespace lsm {

// ========== VersionEdit的编码/解码 ==========
// 编码标签
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  kPrevLogNumber = 8
};

void VersionEdit::EncodeTo(std::string* dst) const {
  if (!comparator_name.empty()) {
    util::PutVarint32(dst, kComparator);
    util::PutLengthPrefixedSlice(dst, comparator_name);
  }
  if (log_number != 0) {
    util::PutVarint32(dst, kLogNumber);
    util::PutVarint64(dst, log_number);
  }
  if (prev_log_number != 0) {
    util::PutVarint32(dst, kPrevLogNumber);
    util::PutVarint64(dst, prev_log_number);
  }
  if (next_file_number != 0) {
    util::PutVarint32(dst, kNextFileNumber);
    util::PutVarint64(dst, next_file_number);
  }
  if (last_sequence != 0) {
    util::PutVarint32(dst, kLastSequence);
    util::PutVarint64(dst, last_sequence);
  }
  for (const auto& df : deleted_files) {
    util::PutVarint32(dst, kDeletedFile);
    util::PutVarint32(dst, df.level);
    util::PutVarint64(dst, df.number);
  }
  for (const auto& nf : new_files) {
    util::PutVarint32(dst, kNewFile);
    util::PutVarint32(dst, nf.level);
    util::PutVarint64(dst, nf.file->number);
    util::PutVarint64(dst, nf.file->file_size);
    util::PutLengthPrefixedSlice(dst, nf.file->smallest_key);
    util::PutLengthPrefixedSlice(dst, nf.file->largest_key);
  }
}

Status VersionEdit::DecodeFrom(Slice* src) {
  Clear();
  while (!src->empty()) {
    uint32_t tag;
    if (!util::GetVarint32(src, &tag)) {
      return Status::Corruption("bad Tag in VersionEdit");
    }

    switch (tag) {
      case kComparator: {
        Slice v;
        if (!util::GetLengthPrefixedSlice(src, &v)) {
          return Status::Corruption("bad comparator");
        }
        comparator_name.assign(v.data(), v.size());
        break;
      }
      case kLogNumber: {
        uint64_t v;
        if (!util::GetVarint64(src, &v)) {
          return Status::Corruption("bad log number");
        }
        log_number = v;
        break;
      }
      case kPrevLogNumber: {
        uint64_t v;
        if (!util::GetVarint64(src, &v)) {
          return Status::Corruption("bad prev log number");
        }
        prev_log_number = v;
        break;
      }
      case kNextFileNumber: {
        uint64_t v;
        if (!util::GetVarint64(src, &v)) {
          return Status::Corruption("bad next file number");
        }
        next_file_number = v;
        break;
      }
      case kLastSequence: {
        uint64_t v;
        if (!util::GetVarint64(src, &v)) {
          return Status::Corruption("bad last sequence");
        }
        last_sequence = v;
        break;
      }
      case kDeletedFile: {
        uint32_t level;
        uint64_t number;
        if (!util::GetVarint32(src, &level) || !util::GetVarint64(src, &number)) {
          return Status::Corruption("bad deleted file");
        }
        DeleteFile(level, number);
        break;
      }
      case kNewFile: {
        uint32_t level;
        uint64_t number;
        uint64_t file_size;
        Slice smallest;
        Slice largest;
        if (!util::GetVarint32(src, &level) || !util::GetVarint64(src, &number) ||
            !util::GetVarint64(src, &file_size) ||
            !util::GetLengthPrefixedSlice(src, &smallest) ||
            !util::GetLengthPrefixedSlice(src, &largest)) {
          return Status::Corruption("bad new file");
        }
        AddFile(level, number, file_size, smallest, largest);
        break;
      }
      default: {
        return Status::Corruption("unknown tag");
      }
    }
  }
  return Status::OK();
}

// ========== Version ==========
Version::Version(VersionSet* vset)
    : vset_(vset), prev_(this), next_(this), refs_(0) {}

Version::~Version() {
  assert(refs_ == 0);
}

Status Version::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  // 从第0层开始查找
  for (int level = 0; level < kNumLevels; level++) {
    const auto& files = files_[level];
    if (files.empty()) continue;

    // 对于第0层，需要遍历所有可能包含key的文件（第0层可能有重叠）
    if (level == 0) {
      for (const auto& f : files) {
        // 使用布隆过滤器快速跳过不存在的
        if (f->smallest_key.compare(key) <= 0 && f->largest_key.compare(key) >= 0) {
          // 在这个文件中查找
          Status s = vset_->table_cache_->Get(options, f->number, f->file_size, key, value);
          if (s.ok() || !s.IsNotFound()) {
            return s;
          }
        }
      }
    } else {
      // 更高层是有序的，二分查找
      // 简化查找：直接找key范围
      for (const auto& f : files) {
        if (f->smallest_key.compare(key) > 0) continue;  // f的最小key > 目标key
        if (f->largest_key.compare(key) < 0) continue;  // f的最大key < 目标key

        Status s = vset_->table_cache_->Get(options, f->number, f->file_size, key, value);
        if (s.ok() || !s.IsNotFound()) {
          return s;
        }
      }
    }
  }
  return Status::NotFound("Key not found");
}



// ========== VersionSet ==========
// 文件名帮助函数
static std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

static std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/MANIFEST-%06llu", (unsigned long long)number);
  return dbname + buf;
}

std::string VersionSet::DescriptorFileName(uint64_t number) {
  return lsm::DescriptorFileName(dbname_, number);
}

VersionSet::VersionSet(const std::string& dbname, const Options& options,
                       TableCache* table_cache)
    : dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      next_file_number_(2),
      manifest_file_number_(0),
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  current_ = new Version(this);
  AppendVersion(current_);
}

VersionSet::~VersionSet() {
  // 清理所有Version
  while (dummy_versions_.next_ != &dummy_versions_) {
    Version* v = dummy_versions_.next_;
    v->prev_->next_ = v->next_;
    v->next_->prev_ = v->prev_;
    delete v;
  }
  delete descriptor_log_;
}

void VersionSet::AppendVersion(Version* v) {
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  dummy_versions_.prev_->next_ = v;
  dummy_versions_.prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit) {
  std::lock_guard<std::mutex> lock(mutex_);

  // 1. 基于当前版本生成新版本
  Version* v = new Version(this);

  // 2. 应用编辑：先复制当前版本的文件
  for (int level = 0; level < Version::kNumLevels; level++) {
    v->files_[level] = current_->files_[level];
  }

  // 3. 先处理删除的文件
  for (const auto& df : edit->deleted_files) {
    auto& level_files = v->files_[df.level];
    bool found = false;
    for (auto it = level_files.begin(); it != level_files.end(); ++it) {
      if ((*it)->number == df.number) {
        // 找到，删除
        (*it)->Unref();
        level_files.erase(it);
        found = true;
        break;
      }
    }
    if (!found) {
      delete v;
      return Status::Corruption("deleted file not found");
    }
  }

  // 4. 添加新文件
  for (const auto& nf : edit->new_files) {
    nf.file->Ref();
    v->files_[nf.level].push_back(nf.file);
  }

  // 5. 更新元数据
  if (edit->next_file_number != 0) {
    next_file_number_ = edit->next_file_number;
  }
  if (edit->log_number != 0) {
    log_number_ = edit->log_number;
  }
  if (edit->prev_log_number != 0) {
    prev_log_number_ = edit->prev_log_number;
  }
  if (edit->last_sequence != 0) {
    last_sequence_ = edit->last_sequence;
  }

  // 6. 写Manifest
  if (descriptor_log_ == nullptr) {
    // 第一次写，创建新Manifest
    manifest_file_number_ = next_file_number_++;
    std::string manifest_fname = DescriptorFileName(manifest_file_number_);
    descriptor_log_ = new std::fstream(manifest_fname, std::ios::out | std::ios::binary);
    if (!*descriptor_log_) {
      delete v;
      return Status::IOError("open MANIFEST failed");
    }

    // 写快照
    VersionEdit snapshot_edit;
    snapshot_edit.SetComparatorName(edit->comparator_name);
    snapshot_edit.SetLogNumber(log_number_);
    snapshot_edit.SetPrevLogNumber(prev_log_number_);
    snapshot_edit.SetNextFileNumber(next_file_number_);
    snapshot_edit.SetLastSequence(last_sequence_);

    // 把当前所有文件都加入到snapshot_edit
    for (int level = 0; level < Version::kNumLevels; level++) {
      for (const auto& file : v->files_[level]) {
        snapshot_edit.AddFile(level, file->number, file->file_size,
                            file->smallest_key, file->largest_key);
      }
    }

    // 编码并写入
    std::string record;
    snapshot_edit.EncodeTo(&record);
    // 简单实现：不做crc等校验，直接写
    descriptor_log_->write(record.data(), record.size());
    if (!*descriptor_log_) {
      delete v;
      return Status::IOError("write MANIFEST failed");
    }
    descriptor_log_->flush();

    // 更新CURRENT
    std::string current_fname = CurrentFileName(dbname_);
    std::fstream current(current_fname, std::ios::out | std::ios::trunc);
    if (!current) {
      delete v;
      return Status::IOError("write CURRENT failed");
    }
    current << "MANIFEST-" << manifest_file_number_ << std::endl;
    current.flush();
  } else {
    // 追加记录
    std::string record;
    edit->EncodeTo(&record);
    descriptor_log_->write(record.data(), record.size());
    if (!*descriptor_log_) {
      delete v;
      return Status::IOError("append MANIFEST failed");
    }
    descriptor_log_->flush();
  }

  // 7. 原子切换current（读写分离）
  Version* old_current = current_;
  current_ = v;
  AppendVersion(v);

  // 旧版本不再使用了，Unref（如果没人用会删除）
  if (old_current) {
    old_current->Unref();
  }

  return Status::OK();
}

Status VersionSet::Recover(bool* save_manifest) {
  std::lock_guard<std::mutex> lock(mutex_);

  // 1. 读取CURRENT，找到Manifest文件名
  std::string current_fname = CurrentFileName(dbname_);
  std::fstream current(current_fname, std::ios::in);
  if (!current) {
    *save_manifest = false;
    return Status::OK();  // 空DB
  }

  std::string manifest_name;
  std::getline(current, manifest_name);
  if (!current) {
    return Status::Corruption("CURRENT file invalid");
  }

  std::string manifest_fname = dbname_ + "/" + manifest_name;
  std::fstream manifest(manifest_fname, std::ios::in | std::ios::binary);
  if (!manifest) {
    return Status::IOError("open MANIFEST failed");
  }

  // 2. 读取并解析Manifest
  // 简化：整个读取到内存（实际工程应按record读取）
  std::string data((std::istreambuf_iterator<char>(manifest)),
                   std::istreambuf_iterator<char>());
  Slice src(data);

  VersionEdit edit;
  while (!src.empty()) {
    Status s = edit.DecodeFrom(&src);
    if (!s.ok()) {
      return s;
    }

    // 应用到当前版本
    if (edit.comparator_name != "leveldb.BytewiseComparator" &&
        !edit.comparator_name.empty()) {
      // 简单处理：假设都是默认comparator
    }
    if (edit.next_file_number != 0) {
      next_file_number_ = std::max(next_file_number_, edit.next_file_number);
    }
    if (edit.log_number != 0) {
      log_number_ = std::max(log_number_, edit.log_number);
    }
    if (edit.last_sequence != 0) {
      last_sequence_ = std::max(last_sequence_, edit.last_sequence);
    }

    // 更新当前版本
    Version* v = new Version(this);
    for (int level = 0; level < Version::kNumLevels; level++) {
      v->files_[level] = current_->files_[level];
    }

    // 处理删除的文件
    for (const auto& df : edit.deleted_files) {
      auto& level_files = v->files_[df.level];
      for (auto it = level_files.begin(); it != level_files.end(); ++it) {
        if ((*it)->number == df.number) {
          (*it)->Unref();
          level_files.erase(it);
          break;
        }
      }
    }

    // 添加新文件
    for (const auto& nf : edit.new_files) {
      nf.file->Ref();
      v->files_[nf.level].push_back(nf.file);
    }

    // 切换current
    Version* old_current = current_;
    current_ = v;
    AppendVersion(v);
    if (old_current) {
      old_current->Unref();
    }
  }

  *save_manifest = true;
  return Status::OK();
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  std::lock_guard<std::mutex> lock(mutex_);
  live->insert(manifest_file_number_);
  for (int level = 0; level < Version::kNumLevels; level++) {
    for (const auto& file : current_->files_[level]) {
      live->insert(file->number);
    }
  }
}

}  // namespace lsm
