#pragma once

#include <atomic>
#include <functional>
#include <thread>

namespace taskflow {

// Turns SIGINT/SIGTERM into an ordinary function call on a dedicated thread,
// so shutdown code runs outside of signal-handler context and may lock,
// allocate and log freely.
//
// Construct it on the main thread before creating any other thread: the
// constructor blocks both signals, and threads inherit the blocked mask from
// their creator. POSIX only.
class SignalWatcher {
 public:
  explicit SignalWatcher(std::function<void()> on_signal);
  ~SignalWatcher();

  SignalWatcher(const SignalWatcher&) = delete;
  SignalWatcher& operator=(const SignalWatcher&) = delete;

 private:
  std::function<void()> on_signal_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

}  // namespace taskflow
