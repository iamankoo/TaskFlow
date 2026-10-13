#include <gtest/gtest.h>

#include <map>

#include "taskflow/common/errors.hpp"
#include "taskflow/config/config.hpp"
#include "test_support.hpp"

namespace taskflow {
namespace {

EnvLookup FakeEnv(std::map<std::string, std::string> values) {
  return [values = std::move(values)](const std::string& name) -> std::optional<std::string> {
    const auto it = values.find(name);
    if (it == values.end()) return std::nullopt;
    return it->second;
  };
}

TEST(ConfigTest, DefaultsAreValid) {
  Config config;
  EXPECT_NO_THROW(config.Validate());
  EXPECT_EQ(config.http.port, 8080);
  EXPECT_EQ(config.postgres.port, 5432);
  EXPECT_EQ(config.redis.port, 6379);
  EXPECT_TRUE(config.app.auto_migrate);
}

TEST(ConfigTest, JsonOverridesOnlyTheKeysItNames) {
  const Config config = Config::FromJson(R"({
    "http": {"port": 9090},
    "postgres": {"host": "db", "pool_size": 3},
    "redis": {"database": 2},
    "logging": {"format": "json"},
    "app": {"auto_migrate": false}
  })");
  EXPECT_EQ(config.http.port, 9090);
  EXPECT_EQ(config.http.host, "0.0.0.0");  // untouched default
  EXPECT_EQ(config.postgres.host, "db");
  EXPECT_EQ(config.postgres.pool_size, 3u);
  EXPECT_EQ(config.redis.database, 2u);
  EXPECT_EQ(config.logging.format, "json");
  EXPECT_FALSE(config.app.auto_migrate);
}

TEST(ConfigTest, UnknownKeysAreRejectedSoTyposFailLoudly) {
  EXPECT_THROW(Config::FromJson(R"({"htpp": {}})"), ConfigError);
  EXPECT_THROW(Config::FromJson(R"({"http": {"prot": 1}})"), ConfigError);
}

TEST(ConfigTest, WrongTypesAreRejected) {
  EXPECT_THROW(Config::FromJson(R"({"http": {"port": "8080"}})"), ConfigError);
  EXPECT_THROW(Config::FromJson(R"({"http": {"port": -1}})"), ConfigError);
  EXPECT_THROW(Config::FromJson(R"({"http": {"port": 70000}})"), ConfigError);
  EXPECT_THROW(Config::FromJson(R"({"app": {"auto_migrate": "yes"}})"), ConfigError);
  EXPECT_THROW(Config::FromJson(R"({"postgres": 5})"), ConfigError);
}

TEST(ConfigTest, MalformedJsonIsRejected) {
  EXPECT_THROW(Config::FromJson("{not json"), ConfigError);
  EXPECT_THROW(Config::FromJson("[1,2,3]"), ConfigError);
}

TEST(ConfigTest, EnvironmentOverridesFileWhichOverridesDefaults) {
  testing::TempDir dir;
  dir.Write("config.json", R"({"http": {"port": 9000, "threads": 4}, "redis": {"host": "from-file"}})");

  const Config config = Config::Load(
      dir.path() / "config.json",
      FakeEnv({{"TASKFLOW_HTTP_PORT", "9100"}, {"TASKFLOW_POSTGRES_HOST", "env-db"}}));

  EXPECT_EQ(config.http.port, 9100);            // env beats file
  EXPECT_EQ(config.http.threads, 4u);           // file beats default
  EXPECT_EQ(config.redis.host, "from-file");    // file beats default
  EXPECT_EQ(config.postgres.host, "env-db");    // env beats default
  EXPECT_EQ(config.postgres.database, "taskflow");  // default
}

TEST(ConfigTest, LoadWithoutFileUsesDefaultsAndEnvironment) {
  const Config config = Config::Load(std::nullopt, FakeEnv({{"TASKFLOW_LOG_LEVEL", "debug"},
                                                           {"TASKFLOW_AUTO_MIGRATE", "false"}}));
  EXPECT_EQ(config.logging.level, "debug");
  EXPECT_FALSE(config.app.auto_migrate);
}

TEST(ConfigTest, MissingFileIsAConfigError) {
  EXPECT_THROW(Config::Load(std::filesystem::path("/nonexistent/taskflow.json"), FakeEnv({})),
               ConfigError);
}

TEST(ConfigTest, MalformedEnvironmentValuesAreRejected) {
  EXPECT_THROW(Config::Load(std::nullopt, FakeEnv({{"TASKFLOW_HTTP_PORT", "eighty"}})), ConfigError);
  EXPECT_THROW(Config::Load(std::nullopt, FakeEnv({{"TASKFLOW_HTTP_PORT", "99999"}})), ConfigError);
  EXPECT_THROW(Config::Load(std::nullopt, FakeEnv({{"TASKFLOW_HTTP_PORT", "80x"}})), ConfigError);
  EXPECT_THROW(Config::Load(std::nullopt, FakeEnv({{"TASKFLOW_AUTO_MIGRATE", "maybe"}})), ConfigError);
}

TEST(ConfigTest, ValidationRejectsOutOfRangeValues) {
  const auto rejects = [](auto mutate) {
    Config config;
    mutate(config);
    return [config] { config.Validate(); };
  };
  EXPECT_THROW(rejects([](Config& c) { c.http.port = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.http.threads = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.postgres.pool_size = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.postgres.host.clear(); })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.redis.pool_size = 1000; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.logging.level = "loud"; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.logging.format = "xml"; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.app.migrations_dir.clear(); })(), ConfigError);
}

TEST(ConfigTest, SchedulerDefaultsAreValidAndSensible) {
  const Config config;
  EXPECT_TRUE(config.scheduler.enabled);
  EXPECT_EQ(config.scheduler.tick_interval_ms, 1000u);
  EXPECT_EQ(config.scheduler.aging_interval_ms, 60000u);
  EXPECT_EQ(config.scheduler.redis_prefix, "taskflow");
  EXPECT_NO_THROW(config.Validate());
}

TEST(ConfigTest, SchedulerSettingsComeFromJsonAndEnvironment) {
  testing::TempDir dir;
  dir.Write("config.json", R"({"scheduler": {"tick_interval_ms": 250, "aging_interval_ms": 0,
                              "max_promotions_per_tick": 10, "redis_prefix": "jobs"}})");
  const Config config = Config::Load(dir.path() / "config.json",
                                     FakeEnv({{"TASKFLOW_SCHEDULER_TICK_INTERVAL_MS", "75"},
                                              {"TASKFLOW_SCHEDULER_ENABLED", "false"}}));
  EXPECT_EQ(config.scheduler.tick_interval_ms, 75u);       // env beats file
  EXPECT_EQ(config.scheduler.aging_interval_ms, 0u);       // file value (aging off)
  EXPECT_EQ(config.scheduler.max_promotions_per_tick, 10u);
  EXPECT_EQ(config.scheduler.redis_prefix, "jobs");
  EXPECT_FALSE(config.scheduler.enabled);
}

TEST(ConfigTest, SchedulerValidationRejectsUnsafeValues) {
  const auto rejects = [](auto mutate) {
    Config config;
    mutate(config);
    return [config] { config.Validate(); };
  };
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.tick_interval_ms = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.max_promotions_per_tick = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.candidates_per_priority = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.aging_interval_ms = 90'000'000; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.reconcile_interval_ticks = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.scan_batch_size = 0; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.lock_ttl_ms = 5; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.redis_prefix = ""; })(), ConfigError);
  EXPECT_THROW(rejects([](Config& c) { c.scheduler.redis_prefix = "has space"; })(), ConfigError);
  EXPECT_THROW(Config::FromJson(R"({"scheduler": {"tick": 5}})"), ConfigError);  // unknown key
}

TEST(ConfigTest, DescribeNeverRevealsSecrets) {
  Config config;
  config.postgres.password = "pg-secret-value";
  config.redis.password = "redis-secret-value";
  const std::string description = config.Describe();
  EXPECT_EQ(description.find("pg-secret-value"), std::string::npos);
  EXPECT_EQ(description.find("redis-secret-value"), std::string::npos);
  EXPECT_NE(description.find("postgres="), std::string::npos);
}

}  // namespace
}  // namespace taskflow
