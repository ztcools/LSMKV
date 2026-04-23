#pragma once

#include <atomic>
#include <cstdint>

namespace lsm {

// 统计指标
struct Metrics {
  // 读写 QPS 计数
  std::atomic<uint64_t> reads{0};
  std::atomic<uint64_t> writes{0};

  // 读写延迟（简单的 P50/P99/P999 计数器，简化实现）
  std::atomic<uint64_t> read_latency_total_ns{0};
  std::atomic<uint64_t> write_latency_total_ns{0};

  // 压缩相关
  std::atomic<uint64_t> blocks_compressed{0};
  std::atomic<uint64_t> blocks_uncompressed{0};

  // 缓存相关
  std::atomic<uint64_t> block_cache_hits{0};
  std::atomic<uint64_t> block_cache_misses{0};
  std::atomic<uint64_t> table_cache_hits{0};
  std::atomic<uint64_t> table_cache_misses{0};

  // 放大率统计
  std::atomic<uint64_t> write_amplification_bytes{0};  // 实际写入字节数
  std::atomic<uint64_t> user_writes_bytes{0};          // 用户请求写入字节数

  // Compaction 统计
  std::atomic<uint64_t> compaction_count{0};
  std::atomic<uint64_t> minor_compaction_count{0};
  std::atomic<uint64_t> major_compaction_count{0};

  Metrics() = default;
  ~Metrics() = default;

  // 重置指标
  void Reset() {
    reads.store(0);
    writes.store(0);
    read_latency_total_ns.store(0);
    write_latency_total_ns.store(0);
    blocks_compressed.store(0);
    blocks_uncompressed.store(0);
    block_cache_hits.store(0);
    block_cache_misses.store(0);
    table_cache_hits.store(0);
    table_cache_misses.store(0);
    write_amplification_bytes.store(0);
    user_writes_bytes.store(0);
    compaction_count.store(0);
    minor_compaction_count.store(0);
    major_compaction_count.store(0);
  }

  // 计算缓存命中率
  double BlockCacheHitRate() const {
    uint64_t total = block_cache_hits.load() + block_cache_misses.load();
    if (total == 0) return 0.0;
    return static_cast<double>(block_cache_hits.load()) / total;
  }

  double TableCacheHitRate() const {
    uint64_t total = table_cache_hits.load() + table_cache_misses.load();
    if (total == 0) return 0.0;
    return static_cast<double>(table_cache_hits.load()) / total;
  }

  // 计算写放大
  double WriteAmplification() const {
    uint64_t user = user_writes_bytes.load();
    if (user == 0) return 1.0;
    return static_cast<double>(write_amplification_bytes.load()) / user;
  }
};

// 全局指标指针
extern Metrics* g_metrics;

// 初始化指标
void InitMetrics();

// 释放指标
void DestroyMetrics();

}  // namespace lsm
