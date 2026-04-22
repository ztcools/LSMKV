#include "merge_iterator.h"
#include <algorithm>

namespace lsm {

// ========== MergeIterator ==========
MergeIterator::MergeIterator(const std::vector<Iterator*>& children)
    : children_(children), current_(nullptr) {
  for (auto c : children) {
    // 检查所有迭代器的状态
    if (!c->status().ok() && !status_.ok()) {
      status_ = c->status();
    }
  }
}

MergeIterator::~MergeIterator() {
  // 这里不负责释放children的生命周期，由外部管理
}

Status MergeIterator::status() const {
  if (!status_.ok()) {
    return status_;
  }
  // 检查所有children的状态
  for (auto c : children_) {
    if (!c->status().ok()) {
      return c->status();
    }
  }
  return Status::OK();
}

void MergeIterator::FindSmallest() {
  current_ = nullptr;
  for (auto c : children_) {
    if (!c->Valid()) {
      continue;
    }
    if (current_ == nullptr) {
      current_ = c;
    } else {
      if (c->key().compare(current_->key()) < 0) {
        current_ = c;
      }
    }
  }
}

void MergeIterator::FindLargest() {
  current_ = nullptr;
  for (auto c : children_) {
    if (!c->Valid()) {
      continue;
    }
    if (current_ == nullptr) {
      current_ = c;
    } else {
      if (c->key().compare(current_->key()) > 0) {
        current_ = c;
      }
    }
  }
}

void MergeIterator::Seek(const Slice& target) {
  // 对所有子迭代器Seek
  for (auto c : children_) {
    c->Seek(target);
  }
  FindSmallest();
}

void MergeIterator::SeekToFirst() {
  for (auto c : children_) {
    c->SeekToFirst();
  }
  FindSmallest();
}

void MergeIterator::SeekToLast() {
  for (auto c : children_) {
    c->SeekToLast();
  }
  FindLargest();
}

void MergeIterator::Next() {
  if (!Valid()) {
    return;
  }

  // 先保存current_移动到下一个，之后再找新的最小值
  current_->Next();
  FindSmallest();
}

void MergeIterator::Prev() {
  if (!Valid()) {
    return;
  }

  current_->Prev();
  FindLargest();
}

}  // namespace lsm
