// minimal_test.cc
#include <iostream>
#include <iomanip>
#include <string>
#include "../src/memtable/memtable.h"
#include "../src/util/slice.h"

int main() {
  std::cout << "=== Minimal MemTable Test ===" << std::endl;

  lsm::MemTable table;
  std::string key = "key1";
  std::string value = "value1";

  std::cout << "Putting key1..." << std::endl;
  table.Put(key, value);
  std::cout << "Put done" << std::endl;

  std::cout << "\n--- All entries ---" << std::endl;
  lsm::MemTable::Iterator* iter = table.NewIterator();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::cout << "  key: " << iter->key().ToString() << " (len=" << iter->key().size() << ")" << std::endl;
    std::cout << "  value: " << iter->value().ToString() << " (len=" << iter->value().size() << ")" << std::endl;
  }
  delete iter;

  std::string result;
  std::cout << "\nGetting key1..." << std::endl;
  bool found = table.Get(key, &result);
  std::cout << "Get done, found=" << (found ? "true" : "false") << std::endl;

  if (found) {
    std::cout << "Result: " << result << std::endl;
  } else {
    std::cout << "Key1 not found!" << std::endl;
  }

  return found ? 0 : 1;
}
