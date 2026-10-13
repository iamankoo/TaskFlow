#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>

namespace taskflow {

struct HttpConfig {
  std::string host = "0.0.0.0";
  std::uint16_t port = 8080;
  unsigned threads = 2;  // Drogon IO threads
};

struct PostgresConfig {
  std::string host = "localhost";
  std::uint16_t port = 5432;
  std::string user = "taskflow";
  std::string password;
  std::string database = "taskflow";
  std::string options;  // libpq "options", e.g. -csearch_path=myschema
  unsigned pool_size = 8;
  unsigned connect_timeout_seconds = 5;
  unsigned acquire_timeout_ms = 5000;
};

struct RedisConfig {
  std::string host = "localhost";
  std::uint16_t port = 6379;
  std::string password;
  unsigned database = 0;
  unsigned pool_size = 4;
  unsigned connect_timeout_ms = 2000;
  unsigned command_timeout_ms = 2000;
  unsigned acquire_timeout_ms = 5000;
};

struct LoggingConfig {
  std::string level = "info";   // trace|debug|info|warn|error|critical|off
  std::string format = "text";  // text|json
};

struct SchedulerConfig {
  bool enabled = true;
  unsigned tick_interval_ms = 1000;        // pause between scheduler ticks
  unsigned max_promotions_per_tick = 500;  // PENDING/RETRY_WAIT -> READY per tick
  unsigned candidates_per_priority = 500;  // due jobs read from PostgreSQL per priority per tick
  // Every interval a job has waited counts as one extra priority level, so LOW cannot
  // starve behind a steady stream of CRITICAL jobs. 0 disables aging (strict priority).
  unsigned aging_interval_ms = 60000;
  unsigned reconcile_interval_ticks = 10;  // PostgreSQL -> Redis repair every N ticks
  unsigned scan_batch_size = 500;          // rows/entries per page while reconciling
  unsigned lock_ttl_ms = 10000;            // lifetime of the reconciliation lock
  std::string redis_prefix = "taskflow";   // namespace for every scheduler key in Redis
};

struct AppConfig {
  bool auto_migrate = true;
  std::string migrations_dir = "migrations";
  unsigned startup_timeout_seconds = 30;  // how long to wait for PostgreSQL/Redis at boot
  unsigned shutdown_grace_ms = 1000;      // /health reports shutting_down this long before exit
};

// Returns the value of an environment variable, or nullopt when unset.
using EnvLookup = std::function<std::optional<std::string>(const std::string&)>;
EnvLookup ProcessEnv();

struct Config {
  HttpConfig http;
  PostgresConfig postgres;
  RedisConfig redis;
  LoggingConfig logging;
  SchedulerConfig scheduler;
  AppConfig app;

  // Precedence: built-in defaults < JSON file (optional) < TASKFLOW_* environment.
  // Throws ConfigError on unreadable/invalid input or failed validation.
  static Config Load(const std::optional<std::filesystem::path>& file,
                     const EnvLookup& env = ProcessEnv());

  // Parses a JSON document on top of the defaults. Unknown keys are rejected so
  // typos fail loudly instead of being ignored. Does not validate.
  static Config FromJson(const std::string& json_text);

  void ApplyEnvironment(const EnvLookup& env);
  void Validate() const;

  // Human-readable summary for startup logs. Never contains secrets.
  std::string Describe() const;
};

}  // namespace taskflow
