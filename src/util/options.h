#pragma once

#include <cstddef>
#include <cstdint>

namespace lsm {

// 压缩类型
enum CompressionType {
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kLZ4Compression = 0x2,
  kZSTDCompression = 0x3
};

// 配置选项
struct Options {
  Options();

  // WriteBuffer大小，默认是4MB
  size_t write_buffer_size = 4 * 1024 * 1024;

  // LSM层数，默认是7层
  int num_levels = 7;

  // L1的目标大小，默认是10MB
  uint64_t level1_size = 10 * 1024 * 1024;

  // 每层之间的倍数，默认是10倍
  double level_multiplier = 10.0;

  // 布隆过滤器的位/键，默认是10位
  int bits_per_key = 10;

  // Block的大小，默认是4KB
  size_t block_size = 4 * 1024;

  // Block restart interval，默认是16
  size_t block_restart_interval = 16;

  // BlockCache的大小，默认是8MB
  size_t block_cache_size = 8 * 1024 * 1024;

  // 压缩类型，默认不压缩
  CompressionType compression = kNoCompression;
};

// 读选项
struct ReadOptions {
  ReadOptions();

  // 是否验证校验和（简化版不实现）
  bool verify_checksums = false;

  // 是否填充缓存
  bool fill_cache = true;
};

// 写选项
struct WriteOptions {
  WriteOptions();

  // 是否同步写
  bool sync = false;
};

}  // namespace lsm
