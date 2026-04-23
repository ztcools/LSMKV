// sst_dump.cc
// SSTable 解析工具
#include <cstdio>
#include <iostream>
#include <string>
#include "sstable/table.h"
#include "util/status.h"
#include "util/options.h"
#include "util/cache.h"

namespace lsm {

void Usage() {
  std::cout << "Usage: sst_dump <sst_file>" << std::endl;
  std::cout << "  --file=<sst_file>  SSTable文件路径" << std::endl;
}

int DumpSSTable(const std::string& filename) {
  Options options;
  std::shared_ptr<Cache> block_cache(NewLRUCache(8 << 20));
  std::unique_ptr<Table> table;

  Status s = Table::Open(options, filename, &table, block_cache);
  if (!s.ok()) {
    std::cerr << "Failed to open SSTable: " << s.ToString() << std::endl;
    return 1;
  }

  std::cout << "SSTable: " << filename << std::endl;
  std::cout << "========================================" << std::endl;

  // 打印所有键值对
  std::unique_ptr<Iterator> iter = table->NewIterator(ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::cout << "  " << iter->key().ToString() << " => " << iter->value().ToString() << std::endl;
  }

  return 0;
}

}  // namespace lsm

int main(int argc, char** argv) {
  if (argc < 2) {
    lsm::Usage();
    return 1;
  }

  std::string filename = argv[1];
  return lsm::DumpSSTable(filename);
}
