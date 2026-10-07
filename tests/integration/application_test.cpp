#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "taskflow/app/application.hpp"
#include "taskflow/common/errors.hpp"
#include "test_support.hpp"

// Successful initialization starts the Drogon singleton and is therefore
// covered by the separate taskflow_http_tests process. These tests cover the
// failure paths of the application lifecycle against real (or missing) services.

namespace taskflow {
namespace {

using namespace std::chrono_literals;

class ApplicationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ = testing::IntegrationConfig();
    config_.app.auto_migrate = false;
    config_.app.startup_timeout_seconds = 1;
    config_.postgres.connect_timeout_seconds = 1;
  }
  Config config_;
};

TEST_F(ApplicationTest, AccessorsAndRunRequireInitialization) {
  Application app(config_);
  EXPECT_THROW(app.postgres(), LifecycleError);
  EXPECT_THROW(app.redis(), LifecycleError);
  EXPECT_THROW(app.health(), LifecycleError);
  EXPECT_THROW(app.Run(), LifecycleError);
  EXPECT_FALSE(app.WaitUntilServing(1ms));
}

TEST_F(ApplicationTest, InitializeFailsWithDatabaseErrorWhenPostgresIsUnreachable) {
  config_.postgres.port = 1;
  Application app(config_);
  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(app.Initialize(), DatabaseError);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 15s);
}

TEST_F(ApplicationTest, InitializeFailsWithRedisErrorWhenRedisIsUnreachable) {
  config_.redis.port = 1;
  Application app(config_);
  EXPECT_THROW(app.Initialize(), RedisError);
}

TEST_F(ApplicationTest, MissingMigrationsDirectoryFailsStartupWhenAutoMigrating) {
  config_.app.auto_migrate = true;
  config_.app.migrations_dir = "/nonexistent/migrations";
  Application app(config_);
  EXPECT_THROW(app.Initialize(), MigrationError);
}

TEST_F(ApplicationTest, ShutdownRequestedWhileWaitingForDependenciesAbortsStartupPromptly) {
  config_.redis.port = 1;
  config_.app.startup_timeout_seconds = 60;  // would wait a minute without the shutdown request
  Application app(config_);

  std::thread requester([&] {
    std::this_thread::sleep_for(300ms);
    app.RequestShutdown();
  });
  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(app.Initialize(), LifecycleError);
  requester.join();

  EXPECT_LT(std::chrono::steady_clock::now() - start, 10s);
  EXPECT_TRUE(app.ShutdownRequested());
}

}  // namespace
}  // namespace taskflow
