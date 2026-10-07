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
