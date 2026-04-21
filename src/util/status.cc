#include "status.h"
#include <cstdint>

namespace lsm {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

Status::Status(Code code, const std::string& msg) {
  const char* type;
  switch (code) {
    case kOk:
      type = "OK";
      break;
    case kNotFound:
      type = "NotFound";
      break;
    case kCorruption:
      type = "Corruption";
      break;
    case kNotSupported:
      type = "Not implemented";
      break;
    case kInvalidArgument:
      type = "Invalid argument";
      break;
    case kIOError:
      type = "IO error";
      break;
    default:
      type = "Unknown error";
      break;
  }
  size_t len = strlen(type);
  size_t msg_len = msg.size();
  char* result = new char[len + msg_len + 12];
  memcpy(result, &len, 4);
  memcpy(result + 4, &msg_len, 4);
  memcpy(result + 8, type, len);
  memcpy(result + 8 + len, msg.data(), msg_len);
  result[8 + len + msg_len] = '\0';
  state_ = result;
}

Status::Code Status::code() const {
  if (state_ == nullptr) {
    return kOk;
  }
  uint32_t len;
  memcpy(&len, state_, sizeof(len));
  if (memcmp(state_ + 8, "OK", len) == 0) {
    return kOk;
  } else if (memcmp(state_ + 8, "NotFound", len) == 0) {
    return kNotFound;
  } else if (memcmp(state_ + 8, "Corruption", len) == 0) {
    return kCorruption;
  } else if (memcmp(state_ + 8, "Not implemented", len) == 0) {
    return kNotSupported;
  } else if (memcmp(state_ + 8, "Invalid argument", len) == 0) {
    return kInvalidArgument;
  } else if (memcmp(state_ + 8, "IO error", len) == 0) {
    return kIOError;
  }
  return kNotSupported;
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    return std::string(state_ + 8);
  }
}

}  // namespace lsm
