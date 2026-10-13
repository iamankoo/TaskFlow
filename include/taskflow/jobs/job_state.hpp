#pragma once

#include <array>
#include <span>
#include <string_view>

namespace taskflow {

// Lifecycle of a job. The transition table in job_state.cpp is the single
// source of truth; the database enforces the same table with a trigger
// (migration 0001) and an integration test proves the two never drift apart.
enum class JobState {
  kPending,    // accepted, not yet runnable (future scheduled_at or unmet prerequisites)
  kReady,      // runnable now; waiting for a worker
  kRunning,    // claimed by a worker and executing
  kCompleted,  // finished successfully (terminal)
  kFailed,     // attempts exhausted or non-retryable failure (final until manually requeued)
  kRetryWait,  // an attempt failed or timed out; waiting for the backoff time before retrying
  kTimeout,    // the final attempt exceeded its timeout (final until manually requeued)
  kCancelled,  // cancelled by request (terminal)
};

inline constexpr std::array<JobState, 8> kAllJobStates = {
    JobState::kPending,   JobState::kReady,   JobState::kRunning, JobState::kCompleted,
    JobState::kFailed,    JobState::kRetryWait, JobState::kTimeout, JobState::kCancelled};

// Canonical upper-case name stored in the database ("RETRY_WAIT").
std::string_view ToString(JobState state) noexcept;

// Throws ValidationError for an unknown name.
JobState ParseJobState(std::string_view text);

// States a freshly created job may start in.
bool IsValidInitialState(JobState state) noexcept;

// True when no automatic transition leaves the state. FAILED and TIMEOUT can
// still be requeued manually (an explicit transition to READY).
bool IsTerminal(JobState state) noexcept;

// The states a job in `from` may move to. Empty for COMPLETED and CANCELLED.
std::span<const JobState> AllowedTransitions(JobState from) noexcept;

bool CanTransition(JobState from, JobState to) noexcept;

// Throws InvalidTransitionError naming both states when the move is not allowed.
void ValidateTransition(JobState from, JobState to);

}  // namespace taskflow
