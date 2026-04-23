// db_bench.cc
// 压测工具
#include <cstdio>
#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <random>
#include "db.h"
#include "util/status.h"
#include "util/options.h"
#include "util/metrics.h"

namespace lsm {

void Usage() {
  std::cout << "Usage: db_bench [options]" << std::endl;
  std::cout << "  --db=<path>       数据库路径 (默认 testdb)" << std::endl;
  std::cout << "  --num=<N>         操作次数 (默认 100000)" << std::endl;
  std::cout << "  --value_size=<N>  Value大小 (默认 100)" << std::endl;
  std::cout << "  --reads           运行读测试" << std::endl;
  std::cout << "  --writes          运行写测试" << std::endl;
}

struct BenchOptions {
  std::string db_path = "testdb";
  int num_ops = 100000;
  int value_size = 100;
  bool do_writes = true;
  bool do_reads = true;
};

std::string RandomString(std::mt19937& rng, int len) {
  static const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string s;
  s.reserve(len);
  for (int i = 0; i < len; ++i) {
    s += charset[rng() % (sizeof(charset) - 1)];
  }
  return s;
}

int RunBenchmark(const BenchOptions& opts) {
  std::cout << "Benchmark Options:" << std::endl;
  std::cout << "  DB: " << opts.db_path << std::endl;
  std::cout << "  Num Ops: " << opts.num_ops << std::endl;
  std::cout << "  Value Size: " << opts.value_size << std::endl;
  std::cout << std::endl;

  // 清理旧DB（简化）

  Options options;
  options.write_buffer_size = 4 << 20;  // 4MB
  options.block_cache_size = 8 << 20;   // 8MB

  DB* db = nullptr;
  Status s = DB::Open(options, opts.db_path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }

  std::mt19937 rng(42);
  std::vector<std::string> keys;
  keys.reserve(opts.num_ops);

  // 写测试
  if (opts.do_writes) {
    std::cout << "=== Write Benchmark ===" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < opts.num_ops; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string value = RandomString(rng, opts.value_size);
      keys.push_back(key);
      s = db->Put(WriteOptions(), key, value);
      if (!s.ok()) {
        std::cerr << "Put failed: " << s.ToString() << std::endl;
        delete db;
        return 1;
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "  Time: " << duration << " ms" << std::endl;
    std::cout << "  Throughput: " << (opts.num_ops * 1000.0 / duration) << " ops/sec" << std::endl;
    std::cout << std::endl;
  }

  // 读测试
  if (opts.do_reads) {
    std::cout << "=== Read Benchmark ===" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    std::string value;
    for (int i = 0; i < opts.num_ops; ++i) {
      std::string key = "key" + std::to_string(i);
      s = db->Get(ReadOptions(), key, &value);
      if (!s.ok()) {
        std::cerr << "Get failed: " << s.ToString() << std::endl;
        delete db;
        return 1;
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "  Time: " << duration << " ms" << std::endl;
    std::cout << "  Throughput: " << (opts.num_ops * 1000.0 / duration) << " ops/sec" << std::endl;
    std::cout << std::endl;
  }

  // 打印指标
  if (g_metrics) {
    std::cout << "=== Metrics ===" << std::endl;
    std::cout << "  Reads: " << g_metrics->reads.load() << std::endl;
    std::cout << "  Writes: " << g_metrics->writes.load() << std::endl;
    std::cout << "  Block Cache Hit Rate: " << g_metrics->BlockCacheHitRate() << std::endl;
    std::cout << "  Write Amplification: " << g_metrics->WriteAmplification() << std::endl;
  }

  delete db;
  return 0;
}

}  // namespace lsm

int main(int argc, char** argv) {
  lsm::BenchOptions opts;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.substr(0, 5) == "--db=") {
      opts.db_path = arg.substr(5);
    } else if (arg.substr(0, 6) == "--num=") {
      opts.num_ops = std::stoi(arg.substr(6));
    } else if (arg.substr(0, 13) == "--value_size=") {
      opts.value_size = std::stoi(arg.substr(13));
    } else if (arg == "--writes") {
      opts.do_writes = true;
      opts.do_reads = false;
    } else if (arg == "--reads") {
      opts.do_reads = true;
      opts.do_writes = false;
    } else if (arg == "--help" || arg == "-h") {
      lsm::Usage();
      return 0;
    }
  }
  return lsm::RunBenchmark(opts);
}
