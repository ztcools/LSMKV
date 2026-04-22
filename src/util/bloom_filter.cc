#include "bloom_filter.h"
#include <cmath>
#include <cstring>
#include "util.h"

namespace lsm {

BloomFilter::BloomFilter(double false_positive_rate, size_t expected_elements)
    : num_elements_(0) {
  num_bits_ = CalculateNumBits(false_positive_rate, expected_elements);
  num_probes_ = CalculateNumProbes(false_positive_rate);
  data_.resize((num_bits_ + 7) / 8, 0);
}

BloomFilter::~BloomFilter() = default;

size_t BloomFilter::CalculateNumBits(double fpr, size_t n) {
  if (n == 0) n = 10000;
  double m = -static_cast<double>(n) * log(fpr) / (log(2) * log(2));
  return static_cast<size_t>(m);
}

size_t BloomFilter::CalculateNumProbes(double fpr) {
  double k = log(1 / fpr) / log(2);
  return static_cast<size_t>(std::max(1.0, k));
}

uint32_t BloomFilter::Hash(const Slice& key, uint32_t seed) const {
  return util::Hash(key, seed);
}

void BloomFilter::Add(const Slice& key) {
  if (num_bits_ == 0) return;

  uint32_t h = Hash(key, 0xbc9f1d34);
  const uint32_t delta = (h >> 17) | (h << 15);

  for (size_t i = 0; i < num_probes_; i++) {
    uint32_t bitpos = h % num_bits_;
    data_[bitpos / 8] |= (1 << (bitpos % 8));
    h += delta;
  }
  num_elements_++;
}

bool BloomFilter::MayContain(const Slice& key) const {
  if (num_bits_ == 0) return true;

  uint32_t h = Hash(key, 0xbc9f1d34);
  const uint32_t delta = (h >> 17) | (h << 15);

  for (size_t i = 0; i < num_probes_; i++) {
    uint32_t bitpos = h % num_bits_;
    if ((data_[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
      return false;
    }
    h += delta;
  }
  return true;
}

std::string BloomFilter::Serialize() const {
  std::string result;
  util::PutFixed32(&result, static_cast<uint32_t>(num_bits_));
  util::PutFixed32(&result, static_cast<uint32_t>(num_probes_));
  util::PutFixed64(&result, static_cast<uint64_t>(num_elements_));
  result.append(reinterpret_cast<const char*>(data_.data()), data_.size());
  return result;
}

BloomFilter BloomFilter::Deserialize(const Slice& data) {
  BloomFilter filter;
  Slice input = data;

  uint32_t num_bits;
  if (!util::GetFixed32(&input, &num_bits)) return filter;

  uint32_t num_probes;
  if (!util::GetFixed32(&input, &num_probes)) return filter;

  uint64_t num_elements;
  if (!util::GetFixed64(&input, &num_elements)) return filter;

  filter.num_bits_ = num_bits;
  filter.num_probes_ = num_probes;
  filter.num_elements_ = num_elements;
  filter.data_.resize(input.size());
  memcpy(filter.data_.data(), input.data(), input.size());

  return filter;
}

void BloomFilter::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  num_elements_ = 0;
}

size_t BloomFilter::ApproximateSize() const {
  return data_.size() + 16;
}

}  // namespace lsm
