#pragma once

#include <spdlog/formatter.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <string>
#include <string_view>

#include "taskflow/config/config.hpp"

namespace taskflow {

// One JSON object per line: {"ts":"...Z","level":"info","logger":"taskflow","thread":1,"msg":"..."}
class JsonFormatter final : public spdlog::formatter {
 public:
  void format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) override;
  std::unique_ptr<spdlog::formatter> clone() const override;
};

// Escapes a string for inclusion inside a JSON string literal.
std::string EscapeJson(std::string_view text);

// Configures the process-wide default logger (stdout, text or JSON). Safe to
// call more than once; later calls replace the previous configuration.
void InitLogging(const LoggingConfig& config);

// Flushes and releases logging resources. Call once at process exit.
void ShutdownLogging();

}  // namespace taskflow
