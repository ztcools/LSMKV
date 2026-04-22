#pragma once

#include <vector>
#include "../util/slice.h"
#include "../util/status.h"
#include "../util/iterator.h"

namespace lsm {

// MergeIterator：多路归并
class MergeIterator : public Iterator {
 public:
  explicit MergeIterator(const std::vector<Iterator*>& children);
  ~MergeIterator() override;

  bool Valid() const override { return current_ != nullptr; }
  Slice key() const override { return current_->key(); }
  Slice value() const override { return current_->value(); }

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  Status status() const override;

 private:
  void FindSmallest();
  void FindLargest();

  std::vector<Iterator*> children_;
  Iterator* current_;
  Status status_;
};

}  // namespace lsm
