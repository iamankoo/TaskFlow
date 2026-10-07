#include "taskflow/common/errors.hpp"

namespace taskflow {

std::string_view ToString(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::kInvalidArgument: return "invalid_argument";
    case ErrorCode::kConfig: return "config_error";
    case ErrorCode::kDatabase: return "database_error";
    case ErrorCode::kRedis: return "redis_error";
    case ErrorCode::kMigration: return "migration_error";
    case ErrorCode::kPoolTimeout: return "pool_timeout";
    case ErrorCode::kNotFound: return "not_found";
    case ErrorCode::kConflict: return "conflict";
    case ErrorCode::kInvalidTransition: return "invalid_transition";
    case ErrorCode::kLifecycle: return "lifecycle_error";
    case ErrorCode::kInternal: return "internal_error";
  }
  return "unknown";
}

Error::Error(ErrorCode code, const std::string& message)
    : std::runtime_error(message), code_(code) {}

}  // namespace taskflow
