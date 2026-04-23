#pragma once

#include <string>
#include <memory>
#include <mutex>
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/iterator.h"
#include "util/snapshot.h"
#include "memtable/memtable.h"
#include "memtable/immutable_memtable.h"
#include "wal/wal.h"
#include "version/version.h"
#include "sstable/table_cache.h"
#include "compaction/compaction.h"

namespace lsm {

// DB：LSM-Tree的完整实现
class DB {
 public:
  // 打开/创建DB
  static Status Open(const Options& options, const std::string& dbname,
                     DB** dbptr);

  ~DB();

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  // 写数据
  Status Put(const WriteOptions& options, const Slice& key, const Slice& value);

  // 删除数据
  Status Delete(const WriteOptions& options, const Slice& key);

  // 批量写
  Status Write(const WriteOptions& options, WriteBatch* batch);

  // 读数据
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

  // 获取新的Iterator
  Iterator* NewIterator(const ReadOptions& options);

  // 获取快照
  const Snapshot* GetSnapshot();

  // 释放快照
  void ReleaseSnapshot(const Snapshot* snapshot);

 private:
  DB(const Options& options, const std::string& dbname);

  // 内部辅助：强制刷盘MemTable到Immutable（Minor Compaction）
  Status MakeRoomForWrite(bool force);

  // 内部辅助：可能触发Compaction
  Status MaybeScheduleCompaction();

  // 元信息
  const Options options_;
  const std::string dbname_;

  // 锁
  mutable std::mutex mutex_;

  // WAL日志
  std::unique_ptr<WAL> wal_;

  // MemTable
  std::unique_ptr<MemTable> mem_;

  // Immutable MemTable（正在刷盘的）
  std::unique_ptr<ImmutableMemTable> imm_;

  // BlockCache
  std::shared_ptr<Cache> block_cache_;

  // SSTable缓存
  std::unique_ptr<TableCache> table_cache_;

  // 版本管理
  std::unique_ptr<VersionSet> versions_;

  // 文件删除器
  std::unique_ptr<FileDeleter> file_deleter_;

  // 序列号
  uint64_t sequence_number_ = 0;

  // 下一个文件号
  uint64_t next_file_number_ = 2;

  // 正在运行的Compaction
  bool compaction_in_progress_ = false;
};

}  // namespace lsm
