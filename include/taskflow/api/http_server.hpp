#pragma once

#include <chrono>
#include <functional>
#include <future>
#include <memory>

#include "taskflow/api/health_service.hpp"
#include "taskflow/config/config.hpp"

namespace trantor {
class ConcurrentTaskQueue;
}

namespace taskflow {

// Wraps the Drogon HTTP framework. Drogon is a process-wide singleton that
// cannot be restarted after it stops, so create at most one HttpServer per
// process and call Run() once.
class HttpServer {
 public:
  // Signals are not handled here; see SignalWatcher.
  HttpServer(const HttpConfig& config, HealthService& health);
  ~HttpServer();

  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;

  // Runs the event loop on the calling thread until Quit() is called.
  void Run();

  // Thread-safe. Stops the event loop after `delay` (zero = immediately).
  void Quit(std::chrono::milliseconds delay);

  // Blocks until the server accepts TCP connections on loopback. Returns false on timeout.
  bool WaitUntilRunning(std::chrono::milliseconds timeout);

 private:
  HttpConfig config_;
  // Health probes block on network I/O, so they run off the event-loop threads.
  std::unique_ptr<trantor::ConcurrentTaskQueue> probe_queue_;
  std::promise<void> running_promise_;
  std::shared_future<void> running_;
};

}  // namespace taskflow
