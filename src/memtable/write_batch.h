#pragma once

#include <string>
#include "../util/slice.h"
#include "../util/status.h"

namespace lsm {

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  WriteBatch(const WriteBatch&) = delete;
  WriteBatch& operator=(const WriteBatch&) = delete;

  void Put(const Slice& key, const Slice& value);
  void Delete(const Slice& key);
  void Clear();
  size_t ApproximateSize() const { return rep_.size(); }

  Slice Data() const { return Slice(rep_); }
  void SetContents(Slice contents);

  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };

  Status Iterate(Handler* handler) const;

  int Count() const;
  void SetCount(int count);

 private:
  friend class WriteBatchInternal;
  std::string rep_;
};

}  // namespace lsm
