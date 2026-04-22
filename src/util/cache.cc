#include "cache.h"
#include <cassert>
#include <mutex>
#include "util.h"

namespace lsm {

Cache::~Cache() = default;

namespace {

struct LRUHandle : public Cache::Handle {
  void* value;
  void (*deleter)(const Slice&, void*);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;
  size_t key_length;
  bool in_cache;
  uint32_t refs;
  uint32_t hash;
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }
  void* Value() override { return value; }
};

class LRUCache {
 public:
  LRUCache() : usage_(0), last_id_(0) {
    lru_.next = &lru_;
    lru_.prev = &lru_;
    capacity_ = 0;
  }

  ~LRUCache() {
    for (LRUHandle* e = lru_.next; e != &lru_;) {
      LRUHandle* next = e->next;
      assert(e->in_cache);
      e->in_cache = false;
      assert(e->refs == 1);
      Unref(e);
      e = next;
    }
  }

  void SetCapacity(size_t cap) { capacity_ = cap; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash);

  LRUHandle* Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                    void (*deleter)(const Slice& key, void* value));

  void Release(LRUHandle* handle);

  void Erase(const Slice& key, uint32_t hash);

  uint64_t NewId() {
    std::lock_guard<std::mutex> lk(mutex_);
    return ++last_id_;
  }

  size_t TotalCharge() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return usage_;
  }

  void Prune();

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  size_t capacity_;

  mutable std::mutex mutex_;
  size_t usage_;
  uint64_t last_id_;
  LRUHandle lru_;
  struct Table {
    Table() : length_(0), elems_(0), list_(nullptr) { Resize(); }
    ~Table() { delete[] list_; }

    LRUHandle* Lookup(const Slice& key, uint32_t hash) {
      return *FindPointer(key, hash);
    }

    LRUHandle* Insert(LRUHandle* h) {
      LRUHandle** ptr = FindPointer(h->key(), h->hash);
      LRUHandle* old = *ptr;
      h->next_hash = (old == nullptr ? nullptr : old->next_hash);
      *ptr = h;
      if (old == nullptr) {
        ++elems_;
        if (elems_ > length_) {
          Resize();
        }
      }
      return old;
    }

    LRUHandle* Remove(const Slice& key, uint32_t hash) {
      LRUHandle** ptr = FindPointer(key, hash);
      LRUHandle* result = *ptr;
      if (result != nullptr) {
        *ptr = result->next_hash;
        --elems_;
      }
      return result;
    }

    void Resize() {
      uint32_t new_length = 4;
      while (new_length < elems_) {
        new_length *= 2;
      }
      LRUHandle** new_list = new LRUHandle*[new_length];
      memset(new_list, 0, sizeof(new_list[0]) * new_length);
      uint32_t count = 0;
      for (uint32_t i = 0; i < length_; i++) {
        LRUHandle* h = list_[i];
        while (h != nullptr) {
          LRUHandle* next = h->next_hash;
          uint32_t hash = h->hash;
          LRUHandle** ptr = &new_list[hash & (new_length - 1)];
          h->next_hash = *ptr;
          *ptr = h;
          ++count;
          h = next;
        }
      }
      assert(elems_ == count);
      (void)count;
      delete[] list_;
      list_ = new_list;
      length_ = new_length;
    }

    LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
      LRUHandle** ptr = &list_[hash & (length_ - 1)];
      while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
        ptr = &(*ptr)->next_hash;
      }
      return ptr;
    }

    uint32_t length_;
    uint32_t elems_;
    LRUHandle** list_;
  };

  Table table_;
};

void LRUCache::Ref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) {
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

LRUHandle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  std::lock_guard<std::mutex> lk(mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return e;
}

LRUHandle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             void (*deleter)(const Slice& key, void* value)) {
  std::lock_guard<std::mutex> lk(mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;
  memcpy(e->key_data, key.data(), key.size());

  Ref(e);
  if (capacity_ > 0) {
    e->in_cache = true;
    e->refs++;
    LRU_Append(e);
    usage_ += charge;
    LRUHandle* old = table_.Insert(e);
    if (old != nullptr) {
      FinishErase(old);
    }
    while (usage_ > capacity_ && lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      FinishErase(table_.Remove(old->key(), old->hash));
    }
  }

  return e;
}

void LRUCache::Release(LRUHandle* handle) {
  if (handle == nullptr) {
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mutex_);
    Unref(handle);
  }
}

bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  std::lock_guard<std::mutex> lk(mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  std::lock_guard<std::mutex> lk(mutex_);
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    FinishErase(table_.Remove(e->key(), e->hash));
    e = next;
  }
}

class ShardedLRUCache : public Cache {
 private:
  static const int kNumShards = 16;
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - 4); }

  LRUCache shards_[kNumShards];
  std::mutex id_mutex_;
  uint64_t last_id_;

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shards_[s].SetCapacity(per_shard);
    }
  }

  ~ShardedLRUCache() override = default;

  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = util::Hash(key, 0);
    return reinterpret_cast<Handle*>(
        shards_[Shard(hash)].Insert(key, hash, value, charge, deleter));
  }

  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = util::Hash(key, 0);
    return reinterpret_cast<Handle*>(shards_[Shard(hash)].Lookup(key, hash));
  }

  void Release(Handle* handle) override {
    if (handle == nullptr) return;
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shards_[Shard(h->hash)].Release(h);
  }

  void Erase(const Slice& key) override {
    const uint32_t hash = util::Hash(key, 0);
    shards_[Shard(hash)].Erase(key, hash);
  }

  uint64_t NewId() override {
    std::lock_guard<std::mutex> lk(id_mutex_);
    return ++last_id_;
  }

  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shards_[s].Prune();
    }
  }

  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shards_[s].TotalCharge();
    }
    return total;
  }
};

}  // namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace lsm
