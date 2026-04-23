/**
 * LSM-Tree KV 存储引擎核心接口
 *
 * 作者：zt
 * 邮箱：3614644417@qq.com
 */
#ifndef LSM_DB_H
#define LSM_DB_H

#include <string>
#include <memory>
#include <cstring>  // 包含 strlen

namespace lsm {

/**
 * 表示操作状态的类
 */
class Status {
 public:
  // 构造函数
  Status() : code_(kOk) {}
  explicit Status(int code, const std::string& msg = "")
      : code_(code), msg_(msg) {}

  // 工厂函数
  static Status OK() { return Status(); }
  static Status NotFound(const std::string& msg = "") {
    return Status(kNotFound, msg);
  }
  static Status Corruption(const std::string& msg = "") {
    return Status(kCorruption, msg);
  }
  static Status IOError(const std::string& msg = "") {
    return Status(kIOError, msg);
  }
  static Status InvalidArgument(const std::string& msg = "") {
    return Status(kInvalidArgument, msg);
  }

  // 查询状态
  bool ok() const { return code_ == kOk; }
  bool IsNotFound() const { return code_ == kNotFound; }
  bool IsCorruption() const { return code_ == kCorruption; }
  bool IsIOError() const { return code_ == kIOError; }
  bool IsInvalidArgument() const { return code_ == kInvalidArgument; }

  // 转换为字符串
  std::string ToString() const;

 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kIOError = 3,
    kInvalidArgument = 4
  };
  int code_;
  std::string msg_;
};

/**
 * 字符串切片，用于高效地引用字符串数据
 */
class Slice {
 public:
  Slice() : data_(nullptr), size_(0) {}
  Slice(const char* d, size_t n) : data_(d), size_(n) {}
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
  Slice(const char* s) : data_(s), size_(strlen(s)) {}

  const char* data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  const char& operator[](size_t n) const { return data_[n]; }

  int compare(const Slice& b) const;
  bool operator==(const Slice& b) const;
  bool operator!=(const Slice& b) const;

  std::string ToString() const { return std::string(data_, size_); }

 private:
  const char* data_;
  size_t size_;
};

/**
 * 读操作选项
 */
struct ReadOptions {
  ReadOptions() : verify_checksums(false), fill_cache(true) {}
  bool verify_checksums;  // 是否验证校验和
  bool fill_cache;        // 是否填充缓存
};

/**
 * 写操作选项
 */
struct WriteOptions {
  WriteOptions() : sync(false) {}
  bool sync;  // 是否同步写入
};

/**
 * 数据库配置选项
 */
struct Options {
  Options()
      : create_if_missing(false),
        error_if_exists(false),
        write_buffer_size(4 * 1024 * 1024),
        max_open_files(1000),
        block_cache_size(8 * 1024 * 1024),
        block_size(4 * 1024),
        compression(kNoCompression),
        compression_level(6) {}

  bool create_if_missing;  // 不存在是否创建
  bool error_if_exists;    // 已存在是否报错
  size_t write_buffer_size;  // MemTable 大小
  int max_open_files;      // 最大打开文件数
  size_t block_cache_size;  // Block Cache 大小
  size_t block_size;       // Block 大小

  enum CompressionType {
    kNoCompression = 0,
    kSnappyCompression = 1,
    kZlibCompression = 2,
    kLZ4Compression = 3,
    kZSTDCompression = 4
  };
  CompressionType compression;  // 压缩类型
  int compression_level;        // 压缩级别
};

/**
 * 迭代器接口
 */
class Iterator {
 public:
  Iterator() {}
  virtual ~Iterator() {}

  // 禁用拷贝
  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  // 迭代器是否有效
  virtual bool Valid() const = 0;

  // 移动到第一个 key
  virtual void SeekToFirst() = 0;

  // 移动到最后一个 key
  virtual void SeekToLast() = 0;

  // 定位到 >= target 的位置
  virtual void Seek(const Slice& target) = 0;

  // 移动到下一个 key
  virtual void Next() = 0;

  // 移动到上一个 key
  virtual void Prev() = 0;

  // 当前 key
  virtual Slice key() const = 0;

  // 当前 value
  virtual Slice value() const = 0;

  // 状态
  virtual Status status() const = 0;
};

/**
 * 批量写入
 */
class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  // 插入 Key-Value
  void Put(const Slice& key, const Slice& value);

  // 删除 Key
  void Delete(const Slice& key);

  // 清空
  void Clear();

  // 大小（字节）
  size_t ApproximateSize() const;

 private:
  // 内部实现
  struct Rep;
  std::unique_ptr<Rep> rep_;

  friend class DB;
};

/**
 * 数据库主类
 */
class DB {
 public:
  // 打开一个数据库
  static Status Open(const Options& options, const std::string& name,
                     DB** dbptr);

  DB() = default;
  virtual ~DB() = default;

  // 禁用拷贝
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  // 写入单条记录
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;

  // 删除单条记录
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // 批量写入
  virtual Status Write(const WriteOptions& options, WriteBatch* batch) = 0;

  // 读取单条记录
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  // 返回一个迭代器
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;
};

}  // namespace lsm

#endif  // LSM_DB_H
