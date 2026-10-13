#include "taskflow/jobs/job_state.hpp"

#include <algorithm>
#include <string>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

using S = JobState;

// Keep in sync with the taskflow_guard_job_update trigger (migration 0005; originally 0001).
// PENDING -> FAILED (Phase 4): a job whose prerequisite failed can never run.
constexpr std::array<S, 3> kFromPending = {S::kReady, S::kCancelled, S::kFailed};
constexpr std::array<S, 2> kFromReady = {S::kRunning, S::kCancelled};
constexpr std::array<S, 5> kFromRunning = {S::kCompleted, S::kFailed, S::kRetryWait, S::kTimeout,
                                           S::kCancelled};
constexpr std::array<S, 2> kFromRetryWait = {S::kReady, S::kCancelled};
constexpr std::array<S, 1> kFromFailed = {S::kReady};   // manual requeue
constexpr std::array<S, 1> kFromTimeout = {S::kReady};  // manual requeue

}  // namespace

std::string_view ToString(JobState state) noexcept {
  switch (state) {
    case S::kPending: return "PENDING";
    case S::kReady: return "READY";
    case S::kRunning: return "RUNNING";
    case S::kCompleted: return "COMPLETED";
    case S::kFailed: return "FAILED";
    case S::kRetryWait: return "RETRY_WAIT";
    case S::kTimeout: return "TIMEOUT";
    case S::kCancelled: return "CANCELLED";
  }
  return "UNKNOWN";
}

JobState ParseJobState(std::string_view text) {
  for (const JobState state : kAllJobStates) {
    if (ToString(state) == text) return state;
  }
  throw ValidationError("unknown job state '" + std::string(text) + "'");
}

bool IsValidInitialState(JobState state) noexcept {
  return state == S::kPending || state == S::kReady;
}

bool IsTerminal(JobState state) noexcept {
  switch (state) {
    case S::kCompleted:
    case S::kFailed:
    case S::kTimeout:
    case S::kCancelled: return true;
    default: return false;
  }
}

std::span<const JobState> AllowedTransitions(JobState from) noexcept {
  switch (from) {
    case S::kPending: return kFromPending;
    case S::kReady: return kFromReady;
    case S::kRunning: return kFromRunning;
    case S::kRetryWait: return kFromRetryWait;
    case S::kFailed: return kFromFailed;
    case S::kTimeout: return kFromTimeout;
    case S::kCompleted:
    case S::kCancelled: return {};
  }
  return {};
}

bool CanTransition(JobState from, JobState to) noexcept {
  const auto allowed = AllowedTransitions(from);
  return std::find(allowed.begin(), allowed.end(), to) != allowed.end();
}

void ValidateTransition(JobState from, JobState to) {
  if (!CanTransition(from, to)) {
    throw InvalidTransitionError("job cannot move from " + std::string(ToString(from)) + " to " +
                                 std::string(ToString(to)));
  }
}

}  // namespace taskflow
