#pragma once

#include "slice.h"
#include "status.h"

namespace lsm {

// 迭代器接口
class Iterator {
 public:
  Iterator() = default;
  virtual ~Iterator() = default;

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  virtual bool Valid() const = 0;
  virtual Slice key() const = 0;
  virtual Slice value() const = 0;

  virtual void Seek(const Slice& target) = 0;
  virtual void SeekToFirst() = 0;
  virtual void SeekToLast() = 0;
  virtual void Next() = 0;
  virtual void Prev() = 0;

  virtual Status status() const = 0;
};

}  // namespace lsm
