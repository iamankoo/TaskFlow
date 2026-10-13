#include "taskflow/app/application.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <thread>
#include <utility>

#include "taskflow/common/errors.hpp"
#include "taskflow/db/migration_runner.hpp"
#include "taskflow/version.hpp"

namespace taskflow {

Application::Application(Config config) : config_(std::move(config)) {}

Application::~Application() = default;

PgPool& Application::postgres() {
  if (!postgres_) throw LifecycleError("application is not initialized");
  return *postgres_;
}

RedisClient& Application::redis() {
  if (!redis_) throw LifecycleError("application is not initialized");
  return *redis_;
}

HealthService& Application::health() {
  if (!health_) throw LifecycleError("application is not initialized");
  return *health_;
}

void Application::WaitForDependencies() {
  using Clock = std::chrono::steady_clock;
  const auto deadline = Clock::now() + std::chrono::seconds(config_.app.startup_timeout_seconds);
  auto delay = std::chrono::milliseconds(250);

  const auto wait_for = [&](const char* name, auto&& ping) {
    for (;;) {
      if (shutdown_requested_.load()) throw LifecycleError("shutdown requested during startup");
      try {
        ping();
        spdlog::info("{} is reachable", name);
        return;
      } catch (const Error& e) {
        if (Clock::now() >= deadline) throw;
        spdlog::warn("waiting for {}: {}", name, e.what());
        std::this_thread::sleep_for(delay);
        delay = std::min(delay * 2, std::chrono::milliseconds(2000));
      }
    }
  };

  wait_for("PostgreSQL", [&] { postgres_->Ping(); });
  wait_for("Redis", [&] { redis_->Ping(); });
}

void Application::Initialize() {
  if (http_) throw LifecycleError("application is already initialized");
  spdlog::info("TaskFlow {} starting: {}", kVersion, config_.Describe());

  postgres_ = std::make_unique<PgPool>(config_.postgres);
  redis_ = std::make_unique<RedisClient>(config_.redis);
  WaitForDependencies();

  if (config_.app.auto_migrate) Migrate();

  health_ = std::make_unique<HealthService>(kVersion);
  health_->AddProbe("postgres", [this] { postgres_->Ping(); });
  health_->AddProbe("redis", [this] { redis_->Ping(); });

  if (config_.scheduler.enabled) {
    jobs_ = std::make_unique<JobRepository>(*postgres_);
    schedule_queue_ = std::make_unique<ScheduleQueue>(*redis_, config_.scheduler.redis_prefix);
    dependencies_ = std::make_unique<DependencyService>(*postgres_, *jobs_);
    scheduler_ = std::make_unique<Scheduler>(*jobs_, *schedule_queue_, config_.scheduler);
    scheduler_->SetDependencyService(dependencies_.get());
  }

  http_ = std::make_unique<HttpServer>(config_.http, *health_);
}

void Application::Migrate() {
  MigrationRunner runner(postgres());
  const auto applied = runner.Run(LoadMigrations(config_.app.migrations_dir));
  spdlog::info("migrations complete: {} applied", applied.size());
}

int Application::Run() {
  if (!http_) throw LifecycleError("Run() called before Initialize()");
  if (shutdown_requested_.load()) return 0;  // asked to stop while still starting
  if (scheduler_) scheduler_->Start();
  http_->Run();
  if (scheduler_) scheduler_->Stop();  // finish the current tick, then stop
  spdlog::info("shutdown complete");
  return 0;
}

void Application::RequestShutdown() {
  if (shutdown_requested_.exchange(true)) return;
  spdlog::info("shutdown requested; draining for {} ms", config_.app.shutdown_grace_ms);
  if (health_) health_->SetShuttingDown();
  if (http_) http_->Quit(std::chrono::milliseconds(config_.app.shutdown_grace_ms));
}

bool Application::WaitUntilServing(std::chrono::milliseconds timeout) {
  return http_ && http_->WaitUntilRunning(timeout);
}

}  // namespace taskflow
