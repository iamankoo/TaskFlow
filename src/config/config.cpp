#include "taskflow/config/config.hpp"

#include <json/json.h>

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <sstream>
#include <string_view>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

// ---------- JSON helpers ----------

void RejectUnknownKeys(const Json::Value& object, const std::string& section,
                       std::initializer_list<std::string_view> allowed) {
  for (const auto& name : object.getMemberNames()) {
    if (std::find(allowed.begin(), allowed.end(), name) == allowed.end()) {
      throw ConfigError("unknown config key '" + (section.empty() ? name : section + "." + name) +
                        "'");
    }
  }
}

const Json::Value* Section(const Json::Value& root, const char* name) {
  if (!root.isMember(name)) return nullptr;
  const Json::Value& value = root[name];
  if (!value.isObject()) throw ConfigError(std::string("config section '") + name + "' must be an object");
  return &value;
}

void ReadString(const Json::Value& object, const std::string& section, const char* key,
                std::string& out) {
  if (!object.isMember(key)) return;
  if (!object[key].isString()) {
    throw ConfigError("config key '" + section + "." + key + "' must be a string");
  }
  out = object[key].asString();
}

void ReadBool(const Json::Value& object, const std::string& section, const char* key, bool& out) {
  if (!object.isMember(key)) return;
  if (!object[key].isBool()) {
    throw ConfigError("config key '" + section + "." + key + "' must be a boolean");
  }
  out = object[key].asBool();
}

template <typename T>
void ReadUnsigned(const Json::Value& object, const std::string& section, const char* key, T& out) {
  if (!object.isMember(key)) return;
  const Json::Value& value = object[key];
  if (!value.isUInt64() ||
      value.asUInt64() > static_cast<Json::UInt64>(std::numeric_limits<T>::max())) {
    throw ConfigError("config key '" + section + "." + key + "' must be a non-negative integer in range");
  }
  out = static_cast<T>(value.asUInt64());
}

// ---------- environment helpers ----------

template <typename T>
void EnvUnsigned(const EnvLookup& env, const char* name, T& out) {
  const auto value = env(name);
  if (!value) return;
  unsigned long long parsed = 0;
  const char* begin = value->data();
  const char* end = begin + value->size();
  const auto [ptr, ec] = std::from_chars(begin, end, parsed);
  if (ec != std::errc{} || ptr != end || parsed > std::numeric_limits<T>::max()) {
    throw ConfigError(std::string("environment variable ") + name +
                      " must be a non-negative integer in range, got '" + *value + "'");
  }
  out = static_cast<T>(parsed);
}

void EnvString(const EnvLookup& env, const char* name, std::string& out) {
  if (const auto value = env(name)) out = *value;
}

void EnvBool(const EnvLookup& env, const char* name, bool& out) {
  const auto value = env(name);
  if (!value) return;
  if (*value == "true" || *value == "1") {
    out = true;
  } else if (*value == "false" || *value == "0") {
    out = false;
  } else {
    throw ConfigError(std::string("environment variable ") + name +
                      " must be true/false/1/0, got '" + *value + "'");
  }
}

void Require(bool condition, const std::string& message) {
  if (!condition) throw ConfigError(message);
}

bool OneOf(const std::string& value, std::initializer_list<const char*> options) {
  return std::any_of(options.begin(), options.end(), [&](const char* o) { return value == o; });
}

}  // namespace

EnvLookup ProcessEnv() {
  return [](const std::string& name) -> std::optional<std::string> {
    const char* value = std::getenv(name.c_str());
    if (value == nullptr) return std::nullopt;
    return std::string(value);
  };
}

Config Config::FromJson(const std::string& json_text) {
  Json::Value root;
  Json::CharReaderBuilder builder;
  builder["allowComments"] = false;
  builder["rejectDupKeys"] = true;
  std::string errors;
  std::istringstream stream(json_text);
  if (!Json::parseFromStream(builder, stream, &root, &errors)) {
    throw ConfigError("invalid config JSON: " + errors);
  }
  if (!root.isObject()) throw ConfigError("config JSON must be an object");
  RejectUnknownKeys(root, "", {"http", "postgres", "redis", "logging", "app"});

  Config config;
  if (const auto* http = Section(root, "http")) {
    RejectUnknownKeys(*http, "http", {"host", "port", "threads"});
    ReadString(*http, "http", "host", config.http.host);
    ReadUnsigned(*http, "http", "port", config.http.port);
    ReadUnsigned(*http, "http", "threads", config.http.threads);
  }
  if (const auto* pg = Section(root, "postgres")) {
    RejectUnknownKeys(*pg, "postgres",
                      {"host", "port", "user", "password", "database", "options", "pool_size",
                       "connect_timeout_seconds", "acquire_timeout_ms"});
    ReadString(*pg, "postgres", "host", config.postgres.host);
    ReadUnsigned(*pg, "postgres", "port", config.postgres.port);
    ReadString(*pg, "postgres", "user", config.postgres.user);
    ReadString(*pg, "postgres", "password", config.postgres.password);
    ReadString(*pg, "postgres", "database", config.postgres.database);
    ReadString(*pg, "postgres", "options", config.postgres.options);
    ReadUnsigned(*pg, "postgres", "pool_size", config.postgres.pool_size);
    ReadUnsigned(*pg, "postgres", "connect_timeout_seconds", config.postgres.connect_timeout_seconds);
    ReadUnsigned(*pg, "postgres", "acquire_timeout_ms", config.postgres.acquire_timeout_ms);
  }
  if (const auto* redis = Section(root, "redis")) {
    RejectUnknownKeys(*redis, "redis",
                      {"host", "port", "password", "database", "pool_size", "connect_timeout_ms",
                       "command_timeout_ms", "acquire_timeout_ms"});
    ReadString(*redis, "redis", "host", config.redis.host);
    ReadUnsigned(*redis, "redis", "port", config.redis.port);
    ReadString(*redis, "redis", "password", config.redis.password);
    ReadUnsigned(*redis, "redis", "database", config.redis.database);
    ReadUnsigned(*redis, "redis", "pool_size", config.redis.pool_size);
    ReadUnsigned(*redis, "redis", "connect_timeout_ms", config.redis.connect_timeout_ms);
    ReadUnsigned(*redis, "redis", "command_timeout_ms", config.redis.command_timeout_ms);
    ReadUnsigned(*redis, "redis", "acquire_timeout_ms", config.redis.acquire_timeout_ms);
  }
  if (const auto* logging = Section(root, "logging")) {
    RejectUnknownKeys(*logging, "logging", {"level", "format"});
    ReadString(*logging, "logging", "level", config.logging.level);
    ReadString(*logging, "logging", "format", config.logging.format);
  }
  if (const auto* app = Section(root, "app")) {
    RejectUnknownKeys(*app, "app",
                      {"auto_migrate", "migrations_dir", "startup_timeout_seconds",
                       "shutdown_grace_ms"});
    ReadBool(*app, "app", "auto_migrate", config.app.auto_migrate);
    ReadString(*app, "app", "migrations_dir", config.app.migrations_dir);
    ReadUnsigned(*app, "app", "startup_timeout_seconds", config.app.startup_timeout_seconds);
    ReadUnsigned(*app, "app", "shutdown_grace_ms", config.app.shutdown_grace_ms);
  }
  return config;
}

void Config::ApplyEnvironment(const EnvLookup& env) {
  EnvString(env, "TASKFLOW_HTTP_HOST", http.host);
  EnvUnsigned(env, "TASKFLOW_HTTP_PORT", http.port);
  EnvUnsigned(env, "TASKFLOW_HTTP_THREADS", http.threads);

  EnvString(env, "TASKFLOW_POSTGRES_HOST", postgres.host);
  EnvUnsigned(env, "TASKFLOW_POSTGRES_PORT", postgres.port);
  EnvString(env, "TASKFLOW_POSTGRES_USER", postgres.user);
  EnvString(env, "TASKFLOW_POSTGRES_PASSWORD", postgres.password);
  EnvString(env, "TASKFLOW_POSTGRES_DATABASE", postgres.database);
  EnvString(env, "TASKFLOW_POSTGRES_OPTIONS", postgres.options);
  EnvUnsigned(env, "TASKFLOW_POSTGRES_POOL_SIZE", postgres.pool_size);
  EnvUnsigned(env, "TASKFLOW_POSTGRES_CONNECT_TIMEOUT_SECONDS", postgres.connect_timeout_seconds);
  EnvUnsigned(env, "TASKFLOW_POSTGRES_ACQUIRE_TIMEOUT_MS", postgres.acquire_timeout_ms);

  EnvString(env, "TASKFLOW_REDIS_HOST", redis.host);
  EnvUnsigned(env, "TASKFLOW_REDIS_PORT", redis.port);
  EnvString(env, "TASKFLOW_REDIS_PASSWORD", redis.password);
  EnvUnsigned(env, "TASKFLOW_REDIS_DATABASE", redis.database);
  EnvUnsigned(env, "TASKFLOW_REDIS_POOL_SIZE", redis.pool_size);
  EnvUnsigned(env, "TASKFLOW_REDIS_CONNECT_TIMEOUT_MS", redis.connect_timeout_ms);
  EnvUnsigned(env, "TASKFLOW_REDIS_COMMAND_TIMEOUT_MS", redis.command_timeout_ms);
  EnvUnsigned(env, "TASKFLOW_REDIS_ACQUIRE_TIMEOUT_MS", redis.acquire_timeout_ms);

  EnvString(env, "TASKFLOW_LOG_LEVEL", logging.level);
  EnvString(env, "TASKFLOW_LOG_FORMAT", logging.format);

  EnvBool(env, "TASKFLOW_AUTO_MIGRATE", app.auto_migrate);
  EnvString(env, "TASKFLOW_MIGRATIONS_DIR", app.migrations_dir);
  EnvUnsigned(env, "TASKFLOW_STARTUP_TIMEOUT_SECONDS", app.startup_timeout_seconds);
  EnvUnsigned(env, "TASKFLOW_SHUTDOWN_GRACE_MS", app.shutdown_grace_ms);
}

void Config::Validate() const {
  Require(!http.host.empty(), "http.host must not be empty");
  Require(http.port != 0, "http.port must be between 1 and 65535");
  Require(http.threads >= 1 && http.threads <= 256, "http.threads must be between 1 and 256");

  Require(!postgres.host.empty(), "postgres.host must not be empty");
  Require(postgres.port != 0, "postgres.port must be between 1 and 65535");
  Require(!postgres.user.empty(), "postgres.user must not be empty");
  Require(!postgres.database.empty(), "postgres.database must not be empty");
  Require(postgres.pool_size >= 1 && postgres.pool_size <= 256,
          "postgres.pool_size must be between 1 and 256");
  Require(postgres.connect_timeout_seconds >= 1, "postgres.connect_timeout_seconds must be >= 1");
  Require(postgres.acquire_timeout_ms >= 1, "postgres.acquire_timeout_ms must be >= 1");

  Require(!redis.host.empty(), "redis.host must not be empty");
  Require(redis.port != 0, "redis.port must be between 1 and 65535");
  Require(redis.database <= 255, "redis.database must be between 0 and 255");
  Require(redis.pool_size >= 1 && redis.pool_size <= 256,
          "redis.pool_size must be between 1 and 256");
  Require(redis.connect_timeout_ms >= 1, "redis.connect_timeout_ms must be >= 1");
  Require(redis.command_timeout_ms >= 1, "redis.command_timeout_ms must be >= 1");
  Require(redis.acquire_timeout_ms >= 1, "redis.acquire_timeout_ms must be >= 1");

  Require(OneOf(logging.level, {"trace", "debug", "info", "warn", "error", "critical", "off"}),
          "logging.level must be one of trace|debug|info|warn|error|critical|off");
  Require(OneOf(logging.format, {"text", "json"}), "logging.format must be text or json");

  Require(!app.migrations_dir.empty(), "app.migrations_dir must not be empty");
  Require(app.startup_timeout_seconds <= 3600, "app.startup_timeout_seconds must be <= 3600");
  Require(app.shutdown_grace_ms <= 60000, "app.shutdown_grace_ms must be <= 60000");
}

Config Config::Load(const std::optional<std::filesystem::path>& file, const EnvLookup& env) {
  Config config;
  if (file) {
    std::ifstream in(*file, std::ios::binary);
    if (!in) throw ConfigError("cannot read config file '" + file->string() + "'");
    std::ostringstream buffer;
    buffer << in.rdbuf();
    config = FromJson(buffer.str());
  }
  config.ApplyEnvironment(env);
  config.Validate();
  return config;
}

std::string Config::Describe() const {
  std::ostringstream out;
  out << "http=" << http.host << ':' << http.port << " threads=" << http.threads
      << " postgres=" << postgres.user << '@' << postgres.host << ':' << postgres.port << '/'
      << postgres.database << " pg_pool=" << postgres.pool_size << " redis=" << redis.host << ':'
      << redis.port << '/' << redis.database << " redis_pool=" << redis.pool_size
      << " log=" << logging.level << '/' << logging.format
      << " auto_migrate=" << (app.auto_migrate ? "true" : "false")
      << " migrations_dir=" << app.migrations_dir;
  return out.str();
}

}  // namespace taskflow
