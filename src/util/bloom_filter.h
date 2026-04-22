#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "slice.h"

namespace lsm {

class BloomFilter {
 public:
  BloomFilter(double false_positive_rate = 0.01, size_t expected_elements = 100000);

  ~BloomFilter();

  void Add(const Slice& key);

  bool MayContain(const Slice& key) const;

  std::string Serialize() const;

  static BloomFilter Deserialize(const Slice& data);

  void Reset();

  size_t ApproximateSize() const;

 private:
  size_t num_elements_;
  size_t num_bits_;
  size_t num_probes_;
  std::vector<uint8_t> data_;

  static size_t CalculateNumBits(double fpr, size_t n);

  static size_t CalculateNumProbes(double fpr);

  uint32_t Hash(const Slice& key, uint32_t seed) const;
};

}  // namespace lsm
