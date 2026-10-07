#pragma once

#include <atomic>
#include <chrono>
#include <memory>

#include "taskflow/api/health_service.hpp"
#include "taskflow/api/http_server.hpp"
#include "taskflow/config/config.hpp"
#include "taskflow/db/postgres.hpp"
#include "taskflow/redis/redis_client.hpp"

namespace taskflow {

// Owns the process lifecycle:
//   Initialize(): wait for PostgreSQL and Redis, run migrations, wire the HTTP server
//   Run():        serve HTTP until a shutdown is requested (signal or RequestShutdown)
//   destruction:  release connections in reverse order of creation
//
// Logging must already be configured (InitLogging) before Initialize().
class Application {
 public:
  explicit Application(Config config);
  ~Application();

  Application(const Application&) = delete;
  Application& operator=(const Application&) = delete;

  // Throws ConfigError/DatabaseError/RedisError/MigrationError. Dependencies
  // that are not reachable are retried until app.startup_timeout_seconds.
  void Initialize();

  // Applies pending migrations (requires Initialize() to have connected). Used
  // by --migrate-only; Initialize() already calls it when app.auto_migrate is set.
  void Migrate();

  // Blocks until shutdown. Returns the process exit code.
  int Run();

  // Thread-safe and idempotent. Marks /health as shutting_down immediately and
  // stops the HTTP server after app.shutdown_grace_ms.
  void RequestShutdown();

  bool ShutdownRequested() const { return shutdown_requested_.load(); }

  bool WaitUntilServing(std::chrono::milliseconds timeout);

  const Config& config() const { return config_; }
  PgPool& postgres();
  RedisClient& redis();
  HealthService& health();

 private:
  void WaitForDependencies();

  Config config_;
  std::unique_ptr<PgPool> postgres_;
  std::unique_ptr<RedisClient> redis_;
  std::unique_ptr<HealthService> health_;
  std::unique_ptr<HttpServer> http_;
  std::atomic<bool> shutdown_requested_{false};
};

}  // namespace taskflow
