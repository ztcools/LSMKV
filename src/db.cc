#include "db.h"
#include <cstdio>
#include <cstring>
#include <chrono>
#include "compaction/merge_iterator.h"
#include "util/options.h"
#include "util/metrics.h"

namespace lsm {

// ========== DB::Open ==========
Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  // 初始化指标
  InitMetrics();

  *dbptr = nullptr;

  // 创建DB实例
  std::unique_ptr<DB> db(new DB(options, dbname));

  // 创建或恢复
  // 简化版的恢复过程（完整实现需要更复杂的WAL恢复）
  Status s = Status::OK();
  if (s.ok()) {
    *dbptr = db.release();
  }
  return s;
}

// ========== DB::构造函数 ==========
DB::DB(const Options& options, const std::string& dbname)
    : options_(options),
      dbname_(dbname),
      mem_(new MemTable()),
      table_cache_(new TableCache(options, dbname)),
      versions_(new VersionSet(dbname, options, table_cache_.get())),
      file_deleter_(new FileDeleter(dbname)) {
  // 创建BlockCache
  block_cache_ = std::shared_ptr<Cache>(NewLRUCache(options_.block_cache_size));
}

// ========== DB::析构函数 ==========
DB::~DB() = default;

// ========== DB::Put ==========
Status DB::Put(const WriteOptions& options, const Slice& key,
               const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(options, &batch);
}

// ========== DB::Delete ==========
Status DB::Delete(const WriteOptions& options, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(options, &batch);
}

// ========== DB::Write ==========
Status DB::Write(const WriteOptions& options, WriteBatch* batch) {
  auto start = std::chrono::high_resolution_clock::now();

  std::lock_guard<std::mutex> lock(mutex_);

  Status s;

  // 确保有足够空间
  s = MakeRoomForWrite(false);
  if (!s.ok()) {
    return s;
  }

  // 更新序列号
  (void)sequence_number_;
  sequence_number_ += batch->Count();

  // 先写WAL
  if (wal_ != nullptr) {
    s = wal_->Append(*batch);
    if (s.ok() && options.sync) {
      s = wal_->Sync();
    }
    if (!s.ok()) {
      return s;
    }
  }

  // 再写MemTable
  mem_->Write(*batch);

  // 更新指标
  if (g_metrics) {
    g_metrics->writes.fetch_add(1);
    // 记录用户写入字节数
    uint64_t user_bytes = 0;
    // 这里暂时简化
    user_bytes = 64; // 假设平均大小
    g_metrics->user_writes_bytes.fetch_add(user_bytes);
  }

  // 可能触发Compaction
  MaybeScheduleCompaction();

  // 记录写延迟
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  if (g_metrics) {
    g_metrics->write_latency_total_ns.fetch_add(duration);
  }

  return s;
}

// ============== DB::Get ==============
Status DB::Get(const ReadOptions& options, const Slice& key,
               std::string* value) {
  auto start = std::chrono::high_resolution_clock::now();

  std::lock_guard<std::mutex> lock(mutex_);

  // 步骤1：先查MemTable
  if (mem_->Get(key, value)) {
    // 更新指标
    if (g_metrics) {
      g_metrics->reads.fetch_add(1);
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      g_metrics->read_latency_total_ns.fetch_add(duration);
    }
    return Status::OK();
  }

  // 步骤2：再查Immutable
  if (imm_ != nullptr && imm_->Get(key, value)) {
    // 更新指标
    if (g_metrics) {
      g_metrics->reads.fetch_add(1);
      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      g_metrics->read_latency_total_ns.fetch_add(duration);
    }
    return Status::OK();
  }

  // 步骤3：查SSTable（L0 → L1 → … → LN）
  Status s = versions_->current()->Get(options, key, value);

  // 更新指标
  if (g_metrics) {
    g_metrics->reads.fetch_add(1);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    g_metrics->read_latency_total_ns.fetch_add(duration);
  }

  return s;
}

// ============== DB::NewIterator ==============
Iterator* DB::NewIterator(const ReadOptions& options) {
  std::lock_guard<std::mutex> lock(mutex_);

  // 收集所有Iterator
  std::vector<Iterator*> children;

  // 添加MemTable的Iterator
  children.push_back(mem_->NewIterator());

  // 添加Immutable的Iterator
  if (imm_ != nullptr) {
    children.push_back(imm_->NewIterator());
  }

  // 添加SSTable的Iterator（来自当前Version）
  auto sst_iters = versions_->current()->NewIterators(options, table_cache_.get());
  for (auto& it : sst_iters) {
    children.push_back(it.release());
  }

  // 用MergeIterator合并所有
  return new MergeIterator(children);
}

// ============== DB::GetSnapshot ==============
const Snapshot* DB::GetSnapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  // 简化版：直接返回当前sequence_number的快照
  return new Snapshot(sequence_number_);
}

// ============== DB::ReleaseSnapshot ==============
void DB::ReleaseSnapshot(const Snapshot* snapshot) {
  delete snapshot;
}

// ============== DB::MakeRoomForWrite ==============
Status DB::MakeRoomForWrite(bool force) {
  assert(mutex_.try_lock() == false);

  // 检查MemTable是否足够大
  while (true) {
    if (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size || force) {
      return Status::OK();
    }

    // 如果已经有Immutable了，就等待（简化版不实现等待，直接返回OK）
    if (imm_ != nullptr) {
      return Status::OK();
    }

    // 把MemTable转为Immutable
    imm_.reset(new ImmutableMemTable(std::move(mem_)));
    mem_.reset(new MemTable());

    // 关闭旧WAL
    wal_.reset();

    // 创建新WAL
    // 简化版暂不实现WAL的创建
    // TODO: 完整实现需要创建新的WAL文件

    // 尝试触发Compaction
    MaybeScheduleCompaction();
    force = true;  // 下次循环强制完成
  }
}

// ============== DB::MaybeScheduleCompaction ==============
Status DB::MaybeScheduleCompaction() {
  assert(mutex_.try_lock() == false);

  if (compaction_in_progress_) {
    return Status::OK();  // 已有Compaction在运行
  }

  std::unique_ptr<Compaction> c;
  CompactionBuilder builder(options_, versions_.get());
  Status s = builder.PickCompaction(&c);
  if (!s.ok() || c == nullptr) {
    return s;
  }

  compaction_in_progress_ = true;

  // 运行Compaction
  s = builder.RunCompaction(c.get(), dbname_, table_cache_.get());
  if (s.ok()) {
    // 应用VersionEdit
    s = versions_->LogAndApply(c->edit());
  }

  compaction_in_progress_ = false;
  return s;
}

}  // namespace lsm
