#include "util.h"
#include <cstring>

namespace lsm {

namespace util {

uint32_t Hash(const char* data, size_t n, uint32_t seed) {
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = seed ^ (n * m);

  while (data + 4 <= limit) {
    uint32_t w;
    memcpy(&w, data, sizeof(w));
    data += 4;
    h += w;
    h *= m;
    h ^= h >> 16;
  }

  switch (limit - data) {
    case 3:
      h += static_cast<unsigned char>(data[2]) << 16;
      [[fallthrough]];
    case 2:
      h += static_cast<unsigned char>(data[1]) << 8;
      [[fallthrough]];
    case 1:
      h += static_cast<unsigned char>(data[0]);
      h *= m;
      h ^= h >> r;
  }

  return h;
}

static uint32_t crc32_table_[256];

static void InitCrc32Table() {
  static bool initialized = false;
  if (initialized) return;
  initialized = true;

  for (uint32_t i = 0; i < 256; i++) {
    uint32_t c = i;
    for (int j = 0; j < 8; j++) {
      if (c & 1) {
        c = 0xedb88320 ^ (c >> 1);
      } else {
        c = c >> 1;
      }
    }
    crc32_table_[i] = c;
  }
}

uint32_t Crc32(const char* data, size_t n) {
  InitCrc32Table();
  uint32_t c = 0xffffffff;
  for (size_t i = 0; i < n; i++) {
    c = crc32_table_[(c ^ static_cast<unsigned char>(data[i])) & 0xff] ^ (c >> 8);
  }
  return c ^ 0xffffffff;
}

uint32_t Crc32Extend(uint32_t initial_crc, const char* data, size_t n) {
  InitCrc32Table();
  uint32_t c = initial_crc ^ 0xffffffff;
  for (size_t i = 0; i < n; i++) {
    c = crc32_table_[(c ^ static_cast<unsigned char>(data[i])) & 0xff] ^ (c >> 8);
  }
  return c ^ 0xffffffff;
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  memcpy(buf, &value, sizeof(buf));
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  memcpy(buf, &value, sizeof(buf));
  dst->append(buf, sizeof(buf));
}

char* EncodeVarint32(char* dst, uint32_t v) {
  static const int B = 128;
  if (v < (1 << 7)) {
    *(dst++) = v;
  } else if (v < (1 << 14)) {
    *(dst++) = v | B;
    *(dst++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(dst++) = v | B;
    *(dst++) = (v >> 7) | B;
    *(dst++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(dst++) = v | B;
    *(dst++) = (v >> 7) | B;
    *(dst++) = (v >> 14) | B;
    *(dst++) = v >> 21;
  } else {
    *(dst++) = v | B;
    *(dst++) = (v >> 7) | B;
    *(dst++) = (v >> 14) | B;
    *(dst++) = (v >> 21) | B;
    *(dst++) = v >> 28;
  }
  return dst;
}

char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  while (v >= B) {
    *(dst++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(dst++) = static_cast<char>(v);
  return dst;
}

void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* DecodeVarint32(const char* p, const char* limit, uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return nullptr;
}

const char* DecodeVarint64(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

bool GetFixed32(Slice* input, uint32_t* value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  memcpy(value, input->data(), sizeof(*value));
  input->remove_prefix(sizeof(*value));
  return true;
}

bool GetFixed64(Slice* input, uint64_t* value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  memcpy(value, input->data(), sizeof(*value));
  input->remove_prefix(sizeof(*value));
  return true;
}

bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = DecodeVarint32(p, limit, value);
  if (q != nullptr) {
    *input = Slice(q, limit - q);
    return true;
  } else {
    return false;
  }
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = DecodeVarint64(p, limit, value);
  if (q != nullptr) {
    *input = Slice(q, limit - q);
    return true;
  } else {
    return false;
  }
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace util

}  // namespace lsm
