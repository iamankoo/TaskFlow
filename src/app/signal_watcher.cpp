#include "taskflow/app/signal_watcher.hpp"

#include <pthread.h>
#include <signal.h>

#include <spdlog/spdlog.h>

#include <ctime>
#include <utility>

namespace taskflow {

SignalWatcher::SignalWatcher(std::function<void()> on_signal) : on_signal_(std::move(on_signal)) {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  thread_ = std::thread([this, set] {
    const timespec poll_interval{0, 200 * 1000 * 1000};  // lets the destructor stop us promptly
    while (!stop_.load()) {
      const int signal_number = sigtimedwait(&set, nullptr, &poll_interval);
      if (signal_number > 0) {
        spdlog::info("received signal {}", signal_number);
        on_signal_();
        return;
      }
    }
  });
}

SignalWatcher::~SignalWatcher() {
  stop_.store(true);
  if (thread_.joinable()) thread_.join();
}

}  // namespace taskflow
