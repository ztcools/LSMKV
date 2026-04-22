#pragma once

#include <string>
#include <fstream>
#include <mutex>
#include "../util/slice.h"
#include "../util/status.h"
#include "../memtable/write_batch.h"

namespace lsm {

class WAL {
 public:
  explicit WAL(const std::string& path);
  ~WAL();

  WAL(const WAL&) = delete;
  WAL& operator=(const WAL&) = delete;

  Status Append(const WriteBatch& batch);
  Status Recover(WriteBatch* batch);
  Status Close();
  Status Sync();

  static Status Truncate(const std::string& path);

 private:
  enum {
    kBlockSize = 32768,
    kHeaderSize = 7,
    kChecksumSize = 4,
    kLengthSize = 2,
    kTypeSize = 1,
  };

  enum RecordType {
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4,
  };

  std::string path_;
  std::fstream file_;
  std::mutex mutex_;
  int block_offset_;
  char* buf_;

  Status WritePhysicalRecord(RecordType type, const char* data, size_t length);
};

}  // namespace lsm
