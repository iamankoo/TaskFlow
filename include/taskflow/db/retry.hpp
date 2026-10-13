#pragma once

#include <chrono>
#include <random>
#include <thread>

#include "taskflow/common/errors.hpp"

namespace taskflow {

// PostgreSQL aborts one of two transactions that wait for each other (SQLSTATE 40P01,
// deadlock_detected) and aborts transactions that cannot be serialized (40001). Both mean
// "nothing was applied, try again". `body` must be a complete transaction that has been
// rolled back when it throws (PgTransaction does that), so a retry starts from scratch.
inline bool IsRetryableTransactionFailure(const DatabaseError& e) {
  return e.sqlstate() == "40P01" || e.sqlstate() == "40001";
}

template <typename F>
auto RetryOnConflict(F&& body, int max_attempts = 5) -> decltype(body()) {
  thread_local std::mt19937 jitter{std::random_device{}()};
  for (int attempt = 1;; ++attempt) {
    try {
      return body();
    } catch (const DatabaseError& e) {
      if (!IsRetryableTransactionFailure(e) || attempt >= max_attempts) throw;
      const int pause_ms = static_cast<int>(5 + jitter() % 20) * attempt;  // randomized, growing
      std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
    }
  }
}

}  // namespace taskflow
