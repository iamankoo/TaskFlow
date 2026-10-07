#include "taskflow/logging/logging.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

namespace taskflow {

void InitLogging(const LoggingConfig& config) {
  auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("taskflow", sink);

  if (config.format == "json") {
    logger->set_formatter(std::make_unique<JsonFormatter>());
  } else {
    logger->set_pattern("%Y-%m-%dT%H:%M:%S.%e%z %^%-5l%$ [%n] %v");
  }

  logger->set_level(spdlog::level::from_str(config.level));
  logger->flush_on(spdlog::level::warn);
  spdlog::set_default_logger(std::move(logger));
}

void ShutdownLogging() {
  spdlog::default_logger()->flush();
  spdlog::shutdown();
}

}  // namespace taskflow
