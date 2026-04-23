// simple_test.cc
// 简单的LSM-Tree测试程序
#include <iostream>
#include <string>
#include "db.h"
#include "util/options.h"
#include "util/status.h"
#include "util/metrics.h"

namespace lsm {

int SimpleTest() {
  std::cout << "=== LSM-Tree Simple Test ===" << std::endl;

  std::string dbname = "test_simple_db";

  // 清理旧DB（简化处理）
  std::cout << "Opening database..." << std::endl;

  Options options;
  options.write_buffer_size = 4 << 20;  // 4MB
  options.block_cache_size = 8 << 20;   // 8MB

  DB* db = nullptr;
  Status s = DB::Open(options, dbname, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << std::endl;
    return 1;
  }

  std::cout << "Writing data..." << std::endl;
  for (int i = 0; i < 10; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    s = db->Put(WriteOptions(), key, value);
    if (!s.ok()) {
      std::cerr << "Put failed: " << s.ToString() << std::endl;
      delete db;
      return 1;
    }
    std::cout << "  Put " << key << " => " << value << std::endl;
  }

  std::cout << "\nReading data..." << std::endl;
  std::string value;
  for (int i = 0; i < 10; ++i) {
    std::string key = "key" + std::to_string(i);
    s = db->Get(ReadOptions(), key, &value);
    if (s.ok()) {
      std::cout << "  Get " << key << " => " << value << std::endl;
    } else {
      std::cerr << "Get failed: " << s.ToString() << std::endl;
      delete db;
      return 1;
    }
  }

  std::cout << "\nDeleting key5..." << std::endl;
  s = db->Delete(WriteOptions(), "key5");
  if (!s.ok()) {
    std::cerr << "Delete failed: " << s.ToString() << std::endl;
  }

  // 尝试读取被删除的键
  std::cout << "\nTrying to get key5..." << std::endl;
  s = db->Get(ReadOptions(), "key5", &value);
  if (s.ok()) {
    std::cerr << "Key5 should be deleted, but got: " << value << std::endl;
  } else {
    std::cout << "  Key5 correctly not found" << std::endl;
  }

  // 打印指标
  std::cout << "\n=== Metrics ===" << std::endl;
  if (g_metrics) {
    std::cout << "  Reads: " << g_metrics->reads.load() << std::endl;
    std::cout << "  Writes: " << g_metrics->writes.load() << std::endl;
    std::cout << "  Block Cache Hit Rate: " << g_metrics->BlockCacheHitRate() << std::endl;
    std::cout << "  Write Amplification: " << g_metrics->WriteAmplification() << std::endl;
  }

  std::cout << "\nTest passed!" << std::endl;
  delete db;
  return 0;
}

}  // namespace lsm

int main() {
  return lsm::SimpleTest();
}
