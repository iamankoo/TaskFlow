#include <gtest/gtest.h>

#include <string>

#include "taskflow/common/errors.hpp"

namespace taskflow {
namespace {

TEST(ErrorsTest, EveryDomainErrorCarriesItsCodeAndMessage) {
  EXPECT_EQ(ConfigError("a").code(), ErrorCode::kConfig);
  EXPECT_EQ(DatabaseError("a").code(), ErrorCode::kDatabase);
  EXPECT_EQ(RedisError("a").code(), ErrorCode::kRedis);
  EXPECT_EQ(MigrationError("a").code(), ErrorCode::kMigration);
  EXPECT_EQ(PoolTimeoutError("a").code(), ErrorCode::kPoolTimeout);
  EXPECT_EQ(LifecycleError("a").code(), ErrorCode::kLifecycle);
  EXPECT_STREQ(DatabaseError("connection refused").what(), "connection refused");
}

TEST(ErrorsTest, DomainErrorsCanBeCaughtAsTheBaseType) {
  try {
    throw RedisError("boom");
  } catch (const Error& e) {
    EXPECT_EQ(e.code(), ErrorCode::kRedis);
    EXPECT_EQ(std::string(e.what()), "boom");
    return;
  }
  FAIL() << "RedisError was not caught as Error";
}

TEST(ErrorsTest, CodeNamesAreStableMachineReadableStrings) {
  EXPECT_EQ(ToString(ErrorCode::kDatabase), "database_error");
  EXPECT_EQ(ToString(ErrorCode::kRedis), "redis_error");
  EXPECT_EQ(ToString(ErrorCode::kConfig), "config_error");
  EXPECT_EQ(ToString(ErrorCode::kPoolTimeout), "pool_timeout");
}

}  // namespace
}  // namespace taskflow
