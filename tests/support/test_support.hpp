#pragma once

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

#include "taskflow/config/config.hpp"

namespace taskflow::testing {

// Settings for the PostgreSQL/Redis instances the integration tests talk to.
// Read from the same TASKFLOW_* variables the application uses, so the tests
// run unchanged on a developer machine, in Docker Compose and in CI.
inline Config IntegrationConfig() {
  Config config;
  config.ApplyEnvironment(ProcessEnv());
  config.postgres.connect_timeout_seconds = 3;
  config.redis.connect_timeout_ms = 1000;
  config.app.startup_timeout_seconds = 5;
  config.Validate();
  return config;
}

// A unique name usable as a schema or Redis key prefix.
inline std::string UniqueName(const std::string& prefix) {
  static std::atomic<int> counter{0};
  return prefix + "_" + std::to_string(::getpid()) + "_" + std::to_string(counter++);
}

// Temporary directory removed on destruction.
class TempDir {
 public:
  TempDir() : path_(std::filesystem::temp_directory_path() / UniqueName("taskflow_test")) {
    std::filesystem::create_directories(path_);
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  const std::filesystem::path& path() const { return path_; }

  void Write(const std::string& filename, const std::string& content) const {
    std::ofstream(path_ / filename, std::ios::binary) << content;
  }

 private:
  std::filesystem::path path_;
};

}  // namespace taskflow::testing
