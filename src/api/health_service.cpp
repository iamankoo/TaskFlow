#include "taskflow/api/health_service.hpp"

#include <spdlog/spdlog.h>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

const char* StatusName(HealthReport::Status status) {
  switch (status) {
    case HealthReport::Status::kOk: return "ok";
    case HealthReport::Status::kDegraded: return "degraded";
    case HealthReport::Status::kShuttingDown: return "shutting_down";
  }
  return "unknown";
}

}  // namespace

Json::Value HealthReport::ToJson() const {
  Json::Value root(Json::objectValue);
  root["status"] = StatusName(status);
  root["version"] = version;
  root["uptime_seconds"] = static_cast<Json::Int64>(uptime.count());
  Json::Value dependencies(Json::objectValue);
  for (const auto& probe : probes) {
    Json::Value entry(Json::objectValue);
    entry["status"] = probe.up ? "up" : "down";
    entry["latency_ms"] = static_cast<Json::Int64>(probe.latency.count());
    if (!probe.up) entry["error"] = probe.detail;
    dependencies[probe.name] = entry;
  }
  root["dependencies"] = dependencies;
  return root;
}

HealthService::HealthService(std::string version)
    : version_(std::move(version)), started_(std::chrono::steady_clock::now()) {}

void HealthService::AddProbe(std::string name, Probe probe) {
  probes_.push_back({std::move(name), std::move(probe)});
}

HealthReport HealthService::Check() const {
  HealthReport report;
  report.version = version_;
  report.uptime = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - started_);

  bool all_up = true;
  for (const auto& named : probes_) {
    ProbeResult result;
    result.name = named.name;
    const auto begin = std::chrono::steady_clock::now();
    try {
      named.probe();
      result.up = true;
    } catch (const Error& e) {
      result.detail = std::string(ToString(e.code()));
      spdlog::warn("health probe '{}' failed: {}", named.name, e.what());
    } catch (const std::exception& e) {
      result.detail = "internal_error";
      spdlog::warn("health probe '{}' failed unexpectedly: {}", named.name, e.what());
    }
    result.latency = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin);
    all_up = all_up && result.up;
    report.probes.push_back(std::move(result));
  }

  if (shutting_down_.load()) {
    report.status = HealthReport::Status::kShuttingDown;
  } else {
    report.status = all_up ? HealthReport::Status::kOk : HealthReport::Status::kDegraded;
  }
  return report;
}

}  // namespace taskflow
