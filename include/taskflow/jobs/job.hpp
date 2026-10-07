#pragma once

#include <json/json.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "taskflow/jobs/job_state.hpp"

namespace taskflow {

using TimePoint = std::chrono::system_clock::time_point;

// Higher value = more urgent. The numeric value is what the database stores,
// so a descending sort on it yields scheduling order.
enum class JobPriority : std::int16_t {
  kLow = 0,
  kNormal = 1,
  kHigh = 2,
  kCritical = 3,
};

std::string_view ToString(JobPriority priority) noexcept;
JobPriority ParseJobPriority(std::string_view text);                // throws ValidationError
JobPriority JobPriorityFromInt(int value);                          // throws ValidationError
constexpr int ToInt(JobPriority priority) noexcept { return static_cast<int>(priority); }

// Strongly typed job identifier (a canonical lower-case UUID). Constructing one
// from arbitrary text goes through Parse(), so an invalid id cannot propagate.
class JobId {
 public:
  JobId() = default;
  // Throws ValidationError unless `text` is a UUID (case-insensitive); stored lower-case.
  static JobId Parse(std::string_view text);

  const std::string& str() const noexcept { return value_; }
  bool empty() const noexcept { return value_.empty(); }
  friend bool operator==(const JobId&, const JobId&) = default;

 private:
  explicit JobId(std::string value) : value_(std::move(value)) {}
  std::string value_;
};

struct RetryPolicy {
  int max_attempts = 3;  // total attempts including the first run
  std::chrono::milliseconds initial_delay{1000};
  double backoff_multiplier = 2.0;
  std::chrono::milliseconds max_delay{300000};

  // Throws ValidationError describing the first violated rule.
  void Validate() const;
  friend bool operator==(const RetryPolicy&, const RetryPolicy&) = default;
};

inline constexpr int kMaxAttemptsLimit = 1000;
inline constexpr std::chrono::milliseconds kMaxTimeout = std::chrono::hours(24 * 7);
inline constexpr std::size_t kMaxPayloadBytes = 1024 * 1024;
inline constexpr std::size_t kMaxTypeLength = 255;
inline constexpr std::size_t kMaxIdempotencyKeyLength = 255;

// Everything a caller chooses when submitting a job. Server-assigned fields
// (id, sequence, timestamps, attempt counter, version) are not part of it.
struct NewJob {
  std::string type;                        // handler name, e.g. "email.send"
  Json::Value payload{Json::objectValue};  // arbitrary JSON object
  Json::Value metadata{Json::objectValue};  // free-form labels (JSON object)
  JobPriority priority = JobPriority::kNormal;
  JobState initial_state = JobState::kPending;  // PENDING or READY
  std::optional<TimePoint> scheduled_at;        // earliest execution time; default = now
  RetryPolicy retry;
  std::chrono::milliseconds timeout{300000};  // per-attempt execution timeout
  std::optional<std::string> idempotency_key;  // persistence only in Phase 2

  // Throws ValidationError. Mirrors (and is stricter than) the database CHECK constraints.
  void Validate() const;
};

struct Job {
  JobId id;
  std::int64_t sequence = 0;  // global submission order (FIFO tie-breaker for the scheduler)
  std::string type;
  Json::Value payload{Json::objectValue};
  Json::Value metadata{Json::objectValue};
  JobPriority priority = JobPriority::kNormal;
  JobState state = JobState::kPending;
  TimePoint scheduled_at;
  RetryPolicy retry;
  std::chrono::milliseconds timeout{0};
  int attempt_count = 0;
  std::optional<std::string> idempotency_key;
  std::optional<std::string> last_error;
  std::int64_t version = 1;  // incremented by the database on every update
  TimePoint created_at;
  TimePoint updated_at;
  std::optional<TimePoint> finished_at;  // set exactly while the job is COMPLETED/FAILED/TIMEOUT/CANCELLED
};

// Fields that may be edited after submission, and only while the job has not
// started running (PENDING, READY or RETRY_WAIT). Unset members are left alone.
struct JobUpdate {
  std::optional<Json::Value> payload;
  std::optional<Json::Value> metadata;
  std::optional<JobPriority> priority;
  std::optional<TimePoint> scheduled_at;
  std::optional<RetryPolicy> retry;
  std::optional<std::chrono::milliseconds> timeout;

  // Optimistic concurrency: when set, the update fails with ConflictError if the
  // stored version differs.
  std::optional<std::int64_t> expected_version;

  void Validate() const;
};

enum class AttemptState {
  kRunning,
  kSucceeded,
  kFailed,
  kTimedOut,
  kCancelled,
};

std::string_view ToString(AttemptState state) noexcept;
AttemptState ParseAttemptState(std::string_view text);  // throws ValidationError

struct JobAttempt {
  std::int64_t id = 0;
  JobId job_id;
  int attempt_number = 0;  // 1-based
  AttemptState state = AttemptState::kRunning;
  TimePoint started_at;
  std::optional<TimePoint> finished_at;
  std::optional<std::string> error_message;
};

// Outcome of one attempt, stored independently of the job row.
struct JobResult {
  std::int64_t id = 0;
  JobId job_id;
  std::int64_t attempt_id = 0;
  int attempt_number = 0;
  bool success = false;
  std::optional<Json::Value> output;       // success only
  std::optional<std::string> error_message;  // failure only
  TimePoint completed_at;
};

// What a worker reports for an attempt (input to storing a result).
struct AttemptOutcome {
  AttemptState state = AttemptState::kSucceeded;  // kSucceeded, kFailed, kTimedOut or kCancelled
  std::optional<Json::Value> output;               // for kSucceeded
  std::optional<std::string> error_message;        // required for every non-success state

  void Validate() const;
  bool success() const noexcept { return state == AttemptState::kSucceeded; }
};

struct JobEvent {
  std::int64_t id = 0;
  JobId job_id;
  std::optional<JobState> from_state;  // empty for the creation event
  JobState to_state = JobState::kPending;
  std::optional<std::string> reason;
  TimePoint created_at;
};

}  // namespace taskflow
