#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "taskflow/db/postgres.hpp"
#include "taskflow/jobs/job.hpp"

namespace taskflow {

// Optional details of a state change.
struct StateChange {
  std::optional<std::string> reason;       // recorded in the job_events audit trail
  std::optional<std::string> last_error;   // stored on the job
  std::optional<TimePoint> scheduled_at;   // new earliest run time (e.g. retry backoff)
  std::optional<JobState> expected_state;  // fail with ConflictError unless the job is in this state
  std::optional<std::int64_t> expected_version;  // optimistic concurrency token
};

// All SQL for the job model lives here. Every method that changes more than one
// row runs in a single PostgreSQL transaction, and every state change is
// validated against the state machine (in C++ first, then again by a database
// trigger) and recorded in job_events.
//
// Errors: ValidationError (bad input, check-constraint violation),
// NotFoundError, ConflictError (duplicate idempotency key, stale version,
// wrong job state for the operation), InvalidTransitionError, DatabaseError
// (anything else), PoolTimeoutError.
//
// Thread-safe: each call borrows its own pooled connection.
class JobRepository {
 public:
  explicit JobRepository(PgPool& pool) : pool_(pool) {}

  // Persists a job (state PENDING or READY) and its creation event.
  // ConflictError if the idempotency key is already used.
  Job CreateJob(const NewJob& job);

  std::optional<Job> GetJob(const JobId& id);
  std::optional<Job> FindJobByIdempotencyKey(std::string_view key);

  // Moves a job along an allowed transition. Not usable for the transitions that
  // need attempt bookkeeping: READY -> RUNNING (use CreateAttempt) and leaving
  // RUNNING for anything but CANCELLED (use StoreResult). Cancelling a RUNNING
  // job also closes its active attempt as CANCELLED, atomically.
  Job UpdateJobState(const JobId& id, JobState to, const StateChange& change = {});

  // Edits submission settings of a job that has not started running.
  Job UpdateJob(const JobId& id, const JobUpdate& update);

  // Starts the next attempt: moves the job READY -> RUNNING, increments
  // attempt_count and inserts the attempt row, atomically. ConflictError when
  // no attempts remain.
  JobAttempt CreateAttempt(const JobId& id);

  std::vector<JobAttempt> GetAttempts(const JobId& id);  // ordered by attempt number

  // Records the outcome of the running attempt: stores the result, closes the
  // attempt, and moves the job to `next_state`, all in one transaction. Either
  // everything is persisted or nothing is.
  //   success            -> COMPLETED
  //   failed             -> RETRY_WAIT (attempts must remain) or FAILED
  //   timed out          -> RETRY_WAIT (attempts must remain) or TIMEOUT
  //   cancelled          -> CANCELLED
  JobResult StoreResult(const JobId& id, int attempt_number, const AttemptOutcome& outcome,
                        JobState next_state, const StateChange& change = {});

  // The most recent result of the job, if any attempt has produced one.
  std::optional<JobResult> GetResult(const JobId& id);
  std::vector<JobResult> GetResults(const JobId& id);  // oldest first

  std::vector<JobEvent> GetEvents(const JobId& id);  // oldest first

 private:
  PgPool& pool_;
};

}  // namespace taskflow
