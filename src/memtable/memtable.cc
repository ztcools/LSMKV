#include "memtable.h"
#include "../util/util.h"

namespace lsm {

namespace {

enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1,
};

}

int MemTable::KeyComparator::operator()(const Slice& a, const Slice& b) const {
  return a.compare(b);
}

static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  util::PutLengthPrefixedSlice(scratch, target);
  return scratch->c_str();
}

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = util::DecodeVarint32(p, p + 5, &len);
  return Slice(p, len);
}

MemTable::MemTable()
    : table_(&arena_, KeyComparator()),
      memory_usage_(0) {}

MemTable::~MemTable() = default;

size_t MemTable::ApproximateMemoryUsage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return arena_.MemoryUsage() + sizeof(MemTable);
}

void MemTable::Put(const Slice& key, const Slice& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + val_size + 8;
  const size_t encoded_len = util::VarintLength(internal_key_size) +
                             internal_key_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = util::EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  util::EncodeFixed32(p, kTypeValue);
  p += 4;
  memcpy(p, value.data(), val_size);
  table_.Insert(Slice(buf, encoded_len));
  memory_usage_ += encoded_len;
}

void MemTable::Delete(const Slice& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t key_size = key.size();
  size_t internal_key_size = key_size + 4;
  const size_t encoded_len = util::VarintLength(internal_key_size) +
                             internal_key_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = util::EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  util::EncodeFixed32(p, kTypeDeletion);
  table_.Insert(Slice(buf, encoded_len));
  memory_usage_ += encoded_len;
}

class MemTableInserter : public WriteBatch::Handler {
 public:
  MemTable* table_;

  explicit MemTableInserter(MemTable* table) : table_(table) {}
  void Put(const Slice& key, const Slice& value) override {
    table_->Put(key, value);
  }
  void Delete(const Slice& key) override {
    table_->Delete(key);
  }
};

void MemTable::Write(const WriteBatch& batch) {
  MemTableInserter inserter(this);
  batch.Iterate(&inserter);
}

bool MemTable::Get(const Slice& key, std::string* value) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string tmp;
  const char* memkey = EncodeKey(&tmp, key);
  SkipList<Slice, KeyComparator>::Iterator iter(&table_);
  iter.Seek(memkey);
  if (iter.Valid()) {
    Slice entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = util::DecodeVarint32(entry.data(), entry.data() + 5, &key_length);
    Slice user_key(key_ptr, key_length - 4);
    if (user_key == key) {
      const char* p = key_ptr + key_length - 4;
      uint32_t tag = util::DecodeFixed32(p);
      p += 4;
      switch (tag) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(p);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          return false;
      }
    }
  }
  return false;
}

MemTable::Iterator::Iterator(const MemTable* table)
    : iter_(&table->table_) {}

MemTable::Iterator::~Iterator() = default;

bool MemTable::Iterator::Valid() const {
  return iter_.Valid();
}

Slice MemTable::Iterator::key() const {
  Slice key = iter_.key();
  uint32_t key_length;
  const char* key_ptr = util::DecodeVarint32(key.data(), key.data() + 5, &key_length);
  return Slice(key_ptr, key_length - 4);
}

Slice MemTable::Iterator::value() const {
  Slice key = iter_.key();
  uint32_t key_length;
  const char* key_ptr = util::DecodeVarint32(key.data(), key.data() + 5, &key_length);
  return GetLengthPrefixedSlice(key_ptr + key_length);
}

void MemTable::Iterator::Next() { iter_.Next(); }
void MemTable::Iterator::Prev() { iter_.Prev(); }
void MemTable::Iterator::Seek(const Slice& k) {
  std::string tmp;
  iter_.Seek(EncodeKey(&tmp, k));
}
void MemTable::Iterator::SeekToFirst() { iter_.SeekToFirst(); }
void MemTable::Iterator::SeekToLast() { iter_.SeekToLast(); }

MemTable::Iterator* MemTable::NewIterator() const {
  return new Iterator(this);
}

}  // namespace lsm
