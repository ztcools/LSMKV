#include <cstdio>
#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <random>
#include <thread>
#include <atomic>
#include "db.h"
#include "util/status.h"
#include "util/options.h"
#include "util/metrics.h"

namespace lsm {

void Usage() {
  std::cout << "Usage: db_bench [options]" << std::endl;
  std::cout << "  --db=<path>          数据库路径 (默认 testdb)" << std::endl;
  std::cout << "  --num=<N>            总操作次数 (默认 100000)" << std::endl;
  std::cout << "  --value_size=<N>     Value 大小 (默认 256B)" << std::endl;
  std::cout << "  --threads=<N>        线程数 (默认 8)" << std::endl;
  std::cout << "  --writes             只测写" << std::endl;
  std::cout << "  --reads              只测读" << std::endl;
}

struct BenchOptions {
  std::string db_path = "testdb";
  int64_t num_ops = 100000;
  int value_size = 256;
  int threads = 8;
  bool do_writes = true;
  bool do_reads = true;
};

std::string RandomString(std::mt19937& rng, int len) {
  static const char charset[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string s;
  s.reserve(len);
  for (int i = 0; i < len; ++i)
    s += charset[rng() % (sizeof(charset)-1)];
  return s;
}

std::atomic<int64_t> g_success(0);

void WriteThread(DB* db, int64_t start, int64_t end, int value_size) {
  std::mt19937 rng(std::random_device{}());
  for (int64_t i = start; i < end; ++i) {
    std::string key = "key_" + std::to_string(i);
    std::string value = RandomString(rng, value_size);
    Status s = db->Put(WriteOptions(), key, value);
    if (s.ok()) g_success++;
  }
}

void ReadThread(DB* db, int64_t start, int64_t end) {
  std::string value;
  for (int64_t i = start; i < end; ++i) {
    std::string key = "key_" + std::to_string(i);
    Status s = db->Get(ReadOptions(), key, &value);
    if (s.ok()) g_success++;
  }
}

int RunBenchmark(const BenchOptions& opts) {
  Options options;
  options.write_buffer_size = 8 << 20;
  options.block_cache_size = 64 << 20;

  DB* db = nullptr;
  Status s = DB::Open(options, opts.db_path, &db);
  if (!s.ok()) {
    std::cerr << "open db failed: " << s.ToString() << std::endl;
    return 1;
  }

  std::cout << "===== LSM-KV Benchmark =====" << std::endl;
  std::cout << "Threads:    " << opts.threads << std::endl;
  std::cout << "Total Ops:  " << opts.num_ops << std::endl;
  std::cout << "Value Size: " << opts.value_size << "B" << std::endl;
  std::cout << std::endl;

  // ====================== 写压测 ======================
  if (opts.do_writes) {
    g_success = 0;
    auto st = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    int64_t per = opts.num_ops / opts.threads;
    for (int i = 0; i < opts.threads; ++i) {
      int64_t beg = i * per;
      int64_t end = (i == opts.threads - 1) ? opts.num_ops : (i + 1) * per;
      threads.emplace_back(WriteThread, db, beg, end, opts.value_size);
    }
    for (auto& t : threads) t.join();

    auto ed = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ed - st).count();
    double qps = opts.num_ops * 1000.0 / ms;

    std::cout << "Write Result:" << std::endl;
    std::cout << "  Time: " << ms << " ms" << std::endl;
    std::cout << "  QPS:  " << (int64_t)qps << " ops/sec" << std::endl;
    std::cout << std::endl;
  }

  // ====================== 读压测 ======================
  if (opts.do_reads) {
    g_success = 0;
    auto st = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    int64_t per = opts.num_ops / opts.threads;
    for (int i = 0; i < opts.threads; ++i) {
      int64_t beg = i * per;
      int64_t end = (i == opts.threads - 1) ? opts.num_ops : (i + 1) * per;
      threads.emplace_back(ReadThread, db, beg, end);
    }
    for (auto& t : threads) t.join();

    auto ed = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ed - st).count();
    double qps = opts.num_ops * 1000.0 / ms;

    std::cout << "Read Result:" << std::endl;
    std::cout << "  Time: " << ms << " ms" << std::endl;
    std::cout << "  QPS:  " << (int64_t)qps << " ops/sec" << std::endl;
    std::cout << std::endl;
  }

  // ====================== 监控指标 ======================
  if (g_metrics) {
    std::cout << "=== Metrics ===" << std::endl;
    std::cout << "Cache Hit Rate: " << g_metrics->BlockCacheHitRate() << "%" << std::endl;
    std::cout << "Write Amplification: " << g_metrics->WriteAmplification() << std::endl;
  }

  delete db;
  return 0;
}

} // namespace lsm

int main(int argc, char** argv) {
  lsm::BenchOptions opts;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.substr(0, 5) == "--db=") opts.db_path = arg.substr(5);
    else if (arg.substr(0, 6) == "--num=") opts.num_ops = std::stoll(arg.substr(6));
    else if (arg.substr(0, 13) == "--value_size=") opts.value_size = std::stoi(arg.substr(13));
    else if (arg.substr(0, 10) == "--threads=") opts.threads = std::stoi(arg.substr(10));
    else if (arg == "--writes") { opts.do_writes = true; opts.do_reads = false; }
    else if (arg == "--reads") { opts.do_reads = true; opts.do_writes = false; }
    else if (arg == "-h" || arg == "--help") { lsm::Usage(); return 0; }
  }
  return lsm::RunBenchmark(opts);
}
