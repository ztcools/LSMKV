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
  // Just encode varint32(target.size) + target for seek
  size_t len = util::VarintLength(target.size()) + target.size();
  scratch->resize(len);
  char* p = util::EncodeVarint32(&(*scratch)[0], target.size());
  memcpy(p, target.data(), target.size());
  return scratch->c_str();
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
  
  // Simplified encoding:
  // [varint32(key_size)] [key (key_size bytes)] [type (1 byte, 1)] 
  // [varint32(val_size)] [val (val_size bytes)]
  size_t encoded_len = util::VarintLength(key_size) + key_size + 1 + 
                       util::VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = util::EncodeVarint32(buf, key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  *p = 1; // type 1 = value
  p +=1;
  p = util::EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  
  table_.Insert(Slice(buf, encoded_len));
  memory_usage_ += encoded_len;
}

void MemTable::Delete(const Slice& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t key_size = key.size();
  
  // Simplified encoding for deletion:
  // [varint32(key_size)] [key (key_size bytes)] [type (1 byte, 0)]
  size_t encoded_len = util::VarintLength(key_size) + key_size +1;
  char* buf = arena_.Allocate(encoded_len);
  char* p = util::EncodeVarint32(buf, key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  *p =0; // type0 = deletion
  
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
  SkipList<Slice, KeyComparator>::Iterator iter(&table_);
  iter.Seek(EncodeKey(&tmp, key));
  
  while (iter.Valid()) {
    Slice entry = iter.key();
    const char* p = entry.data();
    const char* limit = entry.data() + entry.size();
    
    // Decode key length and key
    uint32_t key_len;
    p = util::DecodeVarint32(p, limit, &key_len);
    Slice entry_key(p, key_len);
    p += key_len;
    
    if (entry_key != key) {
      // since keys are sorted, we can break
      break;
    }
    
    // Get type
    uint8_t type = *p;
    p++;
    if (type == 0) {
      return false; // deletion
    } else if (type == 1) {
      // Decode value
      uint32_t val_len;
      p = util::DecodeVarint32(p, limit, &val_len);
      value->assign(p, val_len);
      return true;
    }
    
    iter.Next();
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
  Slice entry = iter_.key();
  const char* p = entry.data();
  const char* limit = entry.data() + entry.size();
  
  uint32_t key_len;
  p = util::DecodeVarint32(p, limit, &key_len);
  return Slice(p, key_len);
}

Slice MemTable::Iterator::value() const {
  Slice entry = iter_.key();
  const char* p = entry.data();
  const char* limit = entry.data() + entry.size();
  
  // skip key
  uint32_t key_len;
  p = util::DecodeVarint32(p, limit, &key_len);
  p += key_len;
  
  // skip type byte
  p += 1;
  
  // get value
  uint32_t val_len;
  p = util::DecodeVarint32(p, limit, &val_len);
  return Slice(p, val_len);
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
