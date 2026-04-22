#include "wal.h"
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>
#include "../util/util.h"

namespace lsm {

WAL::WAL(const std::string& path)
    : path_(path),
      block_offset_(0),
      buf_(new char[kBlockSize]) {
  file_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
}

WAL::~WAL() {
  Close();
  delete[] buf_;
}

Status WAL::WritePhysicalRecord(RecordType t, const char* data, size_t n) {
  const char* p = data;
  size_t left = n;

  while (left > 0) {
    const size_t leftover = kBlockSize - block_offset_;
    assert(leftover > 0);
    if (leftover < kHeaderSize) {
      if (leftover > 0) {
        file_.write(buf_, leftover);
        memset(buf_, 0, kHeaderSize);
        block_offset_ = 0;
      }
    }

    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type = t;
    if (fragment_length == left) {
      if (block_offset_ == 0) {
        type = kFullType;
      } else {
        type = kLastType;
      }
    } else {
      if (block_offset_ == 0) {
        type = kFirstType;
      } else {
        type = kMiddleType;
      }
    }

    char* ptr = buf_ + block_offset_;
    ptr[6] = static_cast<char>(type);
    uint16_t length = static_cast<uint16_t>(fragment_length);
    memcpy(ptr + 4, &length, 2);

    uint32_t crc = util::Crc32(p, fragment_length);
    memcpy(ptr, &crc, 4);
    memcpy(ptr + kHeaderSize, p, fragment_length);

    block_offset_ += kHeaderSize + fragment_length;

    p += fragment_length;
    left -= fragment_length;
  }

  return Status::OK();
}

Status WAL::Append(const WriteBatch& batch) {
  std::lock_guard<std::mutex> lock(mutex_);
  Slice contents = batch.Data();
  Status s = WritePhysicalRecord(kFullType, contents.data(), contents.size());
  if (s.ok()) {
    file_.write(buf_, block_offset_);
    if (!file_) {
      return Status::IOError("WAL write failed");
    }
  }
  return s;
}

Status WAL::Sync() {
  std::lock_guard<std::mutex> lock(mutex_);
  file_.flush();
  if (!file_) {
    return Status::IOError("WAL sync failed");
  }
  return Status::OK();
}

Status WAL::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_.is_open()) {
    file_.close();
  }
  return Status::OK();
}

Status WAL::Recover(WriteBatch* batch) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!file_.is_open()) {
    return Status::OK();
  }

  file_.close();
  file_.open(path_, std::ios::in | std::ios::binary);
  if (!file_) {
    return Status::OK();
  }

  std::string contents;
  char header[kHeaderSize];
  char scratch[kBlockSize];
  bool in_fragmented_record = false;

  while (true) {
    file_.read(header, kHeaderSize);
    if (!file_) {
      break;
    }

    const char* header_ptr = header;
    uint32_t crc = 0;
    memcpy(&crc, header_ptr, 4);
    uint16_t length = 0;
    memcpy(&length, header_ptr + 4, 2);
    uint8_t type = static_cast<uint8_t>(header_ptr[6]);

    if (length > kBlockSize - kHeaderSize) {
      return Status::Corruption("bad record length in WAL");
    }

    file_.read(scratch, length);
    if (!file_) {
      return Status::Corruption("truncated record in WAL");
    }

    uint32_t expected_crc = util::Crc32(scratch, length);
    if (crc != expected_crc) {
      return Status::Corruption("checksum mismatch in WAL");
    }

    const char* p = scratch;
    size_t n = length;

    switch (type) {
      case kFullType:
        if (in_fragmented_record) {
          return Status::Corruption("partial record without end");
        }
        contents.assign(p, p + n);
        batch->SetContents(Slice(contents));
        in_fragmented_record = false;
        break;

      case kFirstType:
        if (in_fragmented_record) {
          return Status::Corruption("partial record without end");
        }
        contents.assign(p, p + n);
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          return Status::Corruption("missing start of fragmented record");
        }
        contents.append(p, n);
        break;

      case kLastType:
        if (!in_fragmented_record) {
          return Status::Corruption("missing start of fragmented record");
        }
        contents.append(p, n);
        batch->SetContents(Slice(contents));
        in_fragmented_record = false;
        break;

      case kZeroType:
        return Status::Corruption("unexpected zero record");
    }
  }

  if (in_fragmented_record) {
    return Status::Corruption("truncated at end of file");
  }

  return Status::OK();
}

Status WAL::Truncate(const std::string& path) {
  if (unlink(path.c_str()) != 0) {
    return Status::IOError("truncate WAL failed");
  }
  return Status::OK();
}

}  // namespace lsm
