#include <spdlog/spdlog.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>

#include "taskflow/app/application.hpp"
#include "taskflow/app/signal_watcher.hpp"
#include "taskflow/common/errors.hpp"
#include "taskflow/config/config.hpp"
#include "taskflow/logging/logging.hpp"
#include "taskflow/version.hpp"

namespace {

constexpr int kExitOk = 0;
constexpr int kExitFailure = 1;
constexpr int kExitUsage = 2;
constexpr int kExitConfig = 3;

struct Options {
  std::optional<std::filesystem::path> config_file;
  bool migrate_only = false;
  bool show_help = false;
  bool show_version = false;
};

void PrintUsage() {
  std::puts(
      "Usage: taskflow [--config FILE] [--migrate-only] [--version] [--help]\n"
      "\n"
      "  --config FILE    JSON configuration file (also TASKFLOW_CONFIG_FILE)\n"
      "  --migrate-only   apply database migrations and exit\n"
      "\n"
      "All settings can be overridden with TASKFLOW_* environment variables;\n"
      "see .env.example for the full list.");
}

// Returns false (after printing the reason) on malformed arguments.
bool ParseArgs(int argc, char** argv, Options& options) {
  if (const char* env_file = std::getenv("TASKFLOW_CONFIG_FILE"); env_file && *env_file) {
    options.config_file = env_file;
  }
  for (int i = 1; i < argc; ++i) {
    const char* arg = argv[i];
    if (std::strcmp(arg, "--help") == 0 || std::strcmp(arg, "-h") == 0) {
      options.show_help = true;
    } else if (std::strcmp(arg, "--version") == 0) {
      options.show_version = true;
    } else if (std::strcmp(arg, "--migrate-only") == 0) {
      options.migrate_only = true;
    } else if (std::strcmp(arg, "--config") == 0) {
      if (i + 1 >= argc) {
        std::fputs("taskflow: --config requires a file path\n", stderr);
        return false;
      }
      options.config_file = argv[++i];
    } else {
      std::fprintf(stderr, "taskflow: unknown argument '%s'\n", arg);
      return false;
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!ParseArgs(argc, argv, options)) {
    PrintUsage();
    return kExitUsage;
  }
  if (options.show_help) {
    PrintUsage();
    return kExitOk;
  }
  if (options.show_version) {
    std::printf("taskflow %s\n", taskflow::kVersion);
    return kExitOk;
  }

  taskflow::Config config;
  try {
    config = taskflow::Config::Load(options.config_file);
  } catch (const taskflow::ConfigError& e) {
    std::fprintf(stderr, "taskflow: configuration error: %s\n", e.what());
    return kExitConfig;
  }

  taskflow::InitLogging(config.logging);

  int exit_code = kExitOk;
  try {
    // --migrate-only runs the migrations itself below, so skip the automatic run at startup.
    if (options.migrate_only) config.app.auto_migrate = false;
    taskflow::Application app(std::move(config));
    // Created before any other thread so every thread inherits the blocked signal mask.
    taskflow::SignalWatcher signals([&app] { app.RequestShutdown(); });
    try {
      app.Initialize();
    } catch (const taskflow::LifecycleError&) {
      if (!app.ShutdownRequested()) throw;
      spdlog::info("stopped during startup");
      taskflow::ShutdownLogging();
      return kExitOk;
    }
    if (options.migrate_only) {
      app.Migrate();
    } else {
      exit_code = app.Run();
    }
  } catch (const taskflow::ConfigError& e) {
    spdlog::critical("configuration error: {}", e.what());
    exit_code = kExitConfig;
  } catch (const taskflow::Error& e) {
    spdlog::critical("{}: {}", taskflow::ToString(e.code()), e.what());
    exit_code = kExitFailure;
  } catch (const std::exception& e) {
    spdlog::critical("unexpected error: {}", e.what());
    exit_code = kExitFailure;
  }

  taskflow::ShutdownLogging();
  return exit_code;
}
