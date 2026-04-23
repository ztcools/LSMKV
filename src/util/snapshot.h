#pragma once

#include <cstdint>

namespace lsm {

// Snapshot：一致性读快照
class Snapshot {
 public:
  Snapshot(uint64_t sequence_number) : sequence_number_(sequence_number) {}
  ~Snapshot() = default;

  // 获取序列号
  uint64_t GetSequenceNumber() const { return sequence_number_; }

 private:
  const uint64_t sequence_number_;
};

}  // namespace lsm
