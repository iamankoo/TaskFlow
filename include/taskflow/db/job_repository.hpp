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

// Keyset position for paging through scheduling entries ordered by (scheduled_at, sequence).
struct ScanCursor {
  TimePoint scheduled_at;
  std::int64_t sequence = 0;
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

  // Same insert, but on a connection that is already inside a transaction the caller owns and
  // commits (used to create a whole workflow atomically). Writes the creation event too.
  Job CreateJobInTransaction(PgConnection& connection, const NewJob& job);

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

  // ----- scheduler persistence (Phase 3). Retrieval and persistence only: the
  // scheduling decisions live in Scheduler. These never load payloads. -----

  // Jobs that are PENDING or RETRY_WAIT with scheduled_at <= now and no unmet prerequisite (every
  // prerequisite COMPLETED), up to `limit_per_priority` per priority, oldest first. Served by idx_jobs_due.
  std::vector<SchedulingEntry> FindDueJobs(TimePoint now, std::size_t limit_per_priority);

  // Moves the given jobs PENDING/RETRY_WAIT -> READY in ONE transaction, but only those that
  // are still waiting, due at `now` and have no unmet prerequisite (re-checked under the row lock). Rows another scheduler holds locked are skipped
  // (FOR UPDATE SKIP LOCKED), so concurrent schedulers never promote a job twice. Writes one
  // job_events row per promotion. Returns the promoted jobs, in the order of `ids`.
  std::vector<SchedulingEntry> PromoteJobsToReady(const std::vector<JobId>& ids, TimePoint now);

  // READY jobs of one priority ordered by (scheduled_at, sequence), strictly after `after`.
  // Served by idx_jobs_ready_queue.
  std::vector<SchedulingEntry> FindReadyJobs(JobPriority priority,
                                             const std::optional<ScanCursor>& after,
                                             std::size_t limit);

  // PENDING and RETRY_WAIT jobs ordered by (scheduled_at, sequence), strictly after `after`.
  std::vector<SchedulingEntry> FindWaitingJobs(const std::optional<ScanCursor>& after,
                                               std::size_t limit);

  // Current scheduling data for the given ids; unknown ids are omitted.
  std::vector<SchedulingEntry> GetSchedulingEntries(const std::vector<JobId>& ids);

 private:
  PgPool& pool_;
};

}  // namespace taskflow
