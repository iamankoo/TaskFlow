#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace taskflow {

enum class ErrorCode {
  kInvalidArgument,
  kConfig,
  kDatabase,
  kRedis,
  kMigration,
  kPoolTimeout,
  kLifecycle,
  kInternal,
};

std::string_view ToString(ErrorCode code) noexcept;

// Base class for every error TaskFlow raises on purpose. Anything else that
// escapes to the top level is treated as an internal bug.
class Error : public std::runtime_error {
 public:
  Error(ErrorCode code, const std::string& message);
  ErrorCode code() const noexcept { return code_; }

 private:
  ErrorCode code_;
};

class ConfigError : public Error {
 public:
  explicit ConfigError(const std::string& message) : Error(ErrorCode::kConfig, message) {}
};

class DatabaseError : public Error {
 public:
  explicit DatabaseError(const std::string& message) : Error(ErrorCode::kDatabase, message) {}
};

class RedisError : public Error {
 public:
  explicit RedisError(const std::string& message) : Error(ErrorCode::kRedis, message) {}
};

class MigrationError : public Error {
 public:
  explicit MigrationError(const std::string& message) : Error(ErrorCode::kMigration, message) {}
};

class PoolTimeoutError : public Error {
 public:
  explicit PoolTimeoutError(const std::string& message)
      : Error(ErrorCode::kPoolTimeout, message) {}
};

class LifecycleError : public Error {
 public:
  explicit LifecycleError(const std::string& message) : Error(ErrorCode::kLifecycle, message) {}
};

}  // namespace taskflow
