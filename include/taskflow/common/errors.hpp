#pragma once

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace taskflow {

enum class ErrorCode {
  kInvalidArgument,
  kConfig,
  kDatabase,
  kRedis,
  kMigration,
  kPoolTimeout,
  kNotFound,
  kConflict,
  kInvalidTransition,
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
  explicit DatabaseError(const std::string& message, std::string sqlstate = "",
                         std::string constraint = "")
      : Error(ErrorCode::kDatabase, message),
        sqlstate_(std::move(sqlstate)),
        constraint_(std::move(constraint)) {}

  // Five-character PostgreSQL SQLSTATE (e.g. "23505" unique_violation); empty for
  // failures that did not come from the server (connection problems).
  const std::string& sqlstate() const noexcept { return sqlstate_; }
  // Name of the violated constraint, when PostgreSQL reports one.
  const std::string& constraint() const noexcept { return constraint_; }

 private:
  std::string sqlstate_;
  std::string constraint_;
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

// Input rejected by domain validation (never reached the database).
class ValidationError : public Error {
 public:
  explicit ValidationError(const std::string& message)
      : Error(ErrorCode::kInvalidArgument, message) {}
};

class NotFoundError : public Error {
 public:
  explicit NotFoundError(const std::string& message) : Error(ErrorCode::kNotFound, message) {}
};

// A uniqueness rule or optimistic-concurrency check failed.
class ConflictError : public Error {
 public:
  explicit ConflictError(const std::string& message) : Error(ErrorCode::kConflict, message) {}
};

class InvalidTransitionError : public Error {
 public:
  explicit InvalidTransitionError(const std::string& message)
      : Error(ErrorCode::kInvalidTransition, message) {}
};

class LifecycleError : public Error {
 public:
  explicit LifecycleError(const std::string& message) : Error(ErrorCode::kLifecycle, message) {}
};

}  // namespace taskflow
