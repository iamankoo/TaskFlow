#pragma once

#include <json/json.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <vector>

namespace taskflow {

struct ProbeResult {
  std::string name;
  bool up = false;
  std::string detail;  // short machine-readable reason when down; never raw error text
  std::chrono::milliseconds latency{0};
};

struct HealthReport {
  enum class Status { kOk, kDegraded, kShuttingDown };

  Status status = Status::kOk;
  std::string version;
  std::chrono::seconds uptime{0};
  std::vector<ProbeResult> probes;

  bool IsHealthy() const { return status == Status::kOk; }
  int HttpStatus() const { return IsHealthy() ? 200 : 503; }
  Json::Value ToJson() const;
};

// Reports liveness plus connectivity to infrastructure dependencies. A probe is
// a callable that returns normally when its dependency is reachable and throws
// otherwise. Probes run synchronously and may block, so callers must not invoke
// Check() from an event-loop thread.
class HealthService {
 public:
  using Probe = std::function<void()>;

  explicit HealthService(std::string version);

  // Register probes before the service is shared with other threads.
  void AddProbe(std::string name, Probe probe);

  // Once set, Check() reports shutting_down (HTTP 503) so load balancers stop
  // routing traffic before the process exits.
  void SetShuttingDown() { shutting_down_.store(true); }

  HealthReport Check() const;

 private:
  struct NamedProbe {
    std::string name;
    Probe probe;
  };

  std::string version_;
  std::chrono::steady_clock::time_point started_;
  std::vector<NamedProbe> probes_;
  std::atomic<bool> shutting_down_{false};
};

}  // namespace taskflow
