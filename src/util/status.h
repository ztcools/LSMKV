#pragma once

#include <string>
#include <cstring>

namespace lsm {

class Status {
 public:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
  };

  Status() noexcept : state_(nullptr) {}
  ~Status() {
    delete[] state_;
  }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept : state_(rhs.state_) {
    rhs.state_ = nullptr;
  }
  Status& operator=(Status&& rhs) noexcept;

  static Status OK() { return Status(); }
  static Status NotFound(const std::string& msg) {
    return Status(kNotFound, msg);
  }
  static Status Corruption(const std::string& msg) {
    return Status(kCorruption, msg);
  }
  static Status NotSupported(const std::string& msg) {
    return Status(kNotSupported, msg);
  }
  static Status InvalidArgument(const std::string& msg) {
    return Status(kInvalidArgument, msg);
  }
  static Status IOError(const std::string& msg) {
    return Status(kIOError, msg);
  }

  bool ok() const { return state_ == nullptr; }
  bool IsNotFound() const { return code() == kNotFound; }
  bool IsCorruption() const { return code() == kCorruption; }
  bool IsNotSupported() const { return code() == kNotSupported; }
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }
  bool IsIOError() const { return code() == kIOError; }

  std::string ToString() const;

 private:
  static const char* CopyState(const char* s);

  Code code() const;

  Status(Code code, const std::string& msg);

  const char* state_;
};

inline Status::Status(const Status& rhs) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
  if (this != &rhs) {
    delete[] state_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this;
}

inline Status& Status::operator=(Status&& rhs) noexcept {
  if (this != &rhs) {
    delete[] state_;
    state_ = rhs.state_;
    rhs.state_ = nullptr;
  }
  return *this;
}

}  // namespace lsm
