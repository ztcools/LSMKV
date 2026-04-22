#include "write_batch.h"
#include "../util/util.h"

namespace lsm {

namespace {

enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1,
};

}

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(8);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  util::PutFixed32(&rep_, kTypeValue);
  util::PutLengthPrefixedSlice(&rep_, key);
  util::PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  util::PutFixed32(&rep_, kTypeDeletion);
  util::PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::SetContents(Slice contents) {
  assert(contents.size() >= 8);
  rep_.assign(contents.data(), contents.size());
}

int WriteBatch::Count() const {
  int count;
  memcpy(&count, rep_.data() + 4, sizeof(count));
  return count;
}

void WriteBatch::SetCount(int count) {
  memcpy(&rep_[4], &count, sizeof(count));
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < 8) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(8);

  Slice key, value;
  while (!input.empty()) {
    uint32_t tag;
    if (!util::GetFixed32(&input, &tag)) {
      return Status::Corruption("bad WriteBatch tag");
    }
    switch (tag) {
      case kTypeValue:
        if (util::GetLengthPrefixedSlice(&input, &key) &&
            util::GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (util::GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  return Status::OK();
}

WriteBatch::Handler::~Handler() = default;

}  // namespace lsm
