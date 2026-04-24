#pragma once

#include <atomic>
#include <random>
#include <cassert>
#include <cstring>
#include "../util/arena.h"

namespace lsm {

template <typename Key, typename Comparator>
class SkipList {
 public:
  struct Node;

  explicit SkipList(Arena* arena, Comparator comparator);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  void Insert(const Key& key);

  bool Contains(const Key& key) const;

  class Iterator {
   public:
    explicit Iterator(const SkipList* list);

    bool Valid() const;
    const Key& key() const;
    void Next();
    void Prev();
    void Seek(const Key& target);
    void SeekToFirst();
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
  };

 private:
  enum { kMaxHeight = 12 };

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (comparator_(a, b) == 0); }
  bool KeyIsAfterNode(const Key& key, Node* n) const;
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;
  Node* FindLessThan(const Key& key) const;
  Node* FindLast() const;

  Arena* const arena_;
  const Comparator comparator_;
  Node* const head_;
  std::atomic<int> max_height_;
  std::mt19937 rnd_;
};

template <typename Key, typename Comparator>
struct SkipList<Key, Comparator>::Node {
  const Key key;
  std::atomic<Node*> next_[1];

  explicit Node(const Key& k) : key(k) {}

  Node* Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_release);
  }
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }
};

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::SkipList(Arena* arena, Comparator comparator)
    : arena_(arena),
      comparator_(comparator),
      head_(NewNode(Key(), kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(key);
}

template <typename Key, typename Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && (rnd_() % kBranching) == 0) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, typename Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  return (n != nullptr) && (comparator_(n->key, key) < 0);
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
  Node* x = head_;
  int level = max_height_.load(std::memory_order_relaxed) - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        level--;
      }
    }
  }
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = max_height_.load(std::memory_order_relaxed) - 1;
  while (true) {
    assert(x == head_ || comparator_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || comparator_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, typename Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLast() const {
  Node* x = head_;
  int level = max_height_.load(std::memory_order_relaxed) - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > max_height_.load(std::memory_order_relaxed)) {
    for (int i = max_height_.load(std::memory_order_relaxed); i < height; i++) {
      prev[i] = head_;
    }
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template <typename Key, typename Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

template <typename Key, typename Comparator>
SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list)
    : list_(list), node_(nullptr) {}

template <typename Key, typename Comparator>
bool SkipList<Key, Comparator>::Iterator::Valid() const { return node_ != nullptr; }

template <typename Key, typename Comparator>
const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::Prev() {
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, typename Comparator>
void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

}  // namespace lsm
