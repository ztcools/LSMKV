#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include "slice.h"

namespace lsm {

namespace util {

uint32_t Hash(const char* data, size_t n, uint32_t seed);

inline uint32_t Hash(const Slice& s, uint32_t seed) {
  return Hash(s.data(), s.size(), seed);
}

uint32_t Crc32(const char* data, size_t n);

inline uint32_t Crc32(const Slice& s) {
  return Crc32(s.data(), s.size());
}

uint32_t Crc32Extend(uint32_t initial_crc, const char* data, size_t n);

inline uint32_t Crc32Extend(uint32_t initial_crc, const Slice& s) {
  return Crc32Extend(initial_crc, s.data(), s.size());
}

void PutFixed32(std::string* dst, uint32_t value);
void PutFixed64(std::string* dst, uint64_t value);
void PutVarint32(std::string* dst, uint32_t value);
void PutVarint64(std::string* dst, uint64_t value);
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

inline void EncodeFixed32(char* buf, uint32_t value) {
  memcpy(buf, &value, sizeof(value));
}

inline void EncodeFixed64(char* buf, uint64_t value) {
  memcpy(buf, &value, sizeof(value));
}

inline uint32_t DecodeFixed32(const char* buf) {
  uint32_t result;
  memcpy(&result, buf, sizeof(result));
  return result;
}

inline uint64_t DecodeFixed64(const char* buf) {
  uint64_t result;
  memcpy(&result, buf, sizeof(result));
  return result;
}

bool GetFixed32(Slice* input, uint32_t* value);
bool GetFixed64(Slice* input, uint64_t* value);
bool GetVarint32(Slice* input, uint32_t* value);
bool GetVarint64(Slice* input, uint64_t* value);
bool GetLengthPrefixedSlice(Slice* input, Slice* result);

char* EncodeVarint32(char* dst, uint32_t v);
char* EncodeVarint64(char* dst, uint64_t v);

const char* DecodeVarint32(const char* p, const char* limit, uint32_t* value);
const char* DecodeVarint64(const char* p, const char* limit, uint64_t* value);

int VarintLength(uint64_t v);

}  // namespace util

}  // namespace lsm
