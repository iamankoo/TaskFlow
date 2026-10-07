#include "taskflow/db/job_repository.hpp"

#include <chrono>
#include <iomanip>
#include <locale>
#include <memory>
#include <sstream>
#include <utility>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

using Param = PgConnection::Param;

// ---------- value conversion ----------

std::string EpochMicros(const std::string& column) {
  return "(extract(epoch from " + column + ") * 1000000)::bigint";
}

// A bound timestamp parameter: microseconds since the Unix epoch, converted exactly in SQL.
std::string TimestampParam(int index) {
  return "(timestamptz 'epoch' + $" + std::to_string(index) +
         "::bigint * interval '1 microsecond')";
}

std::string ToMicros(TimePoint time) {
  return std::to_string(
      std::chrono::duration_cast<std::chrono::microseconds>(time.time_since_epoch()).count());
}

TimePoint FromMicros(const std::string& text) {
  return TimePoint(std::chrono::duration_cast<TimePoint::duration>(
      std::chrono::microseconds(std::stoll(text))));
}

std::string ToJsonText(const Json::Value& value) {
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  return Json::writeString(builder, value);
}

Json::Value ParseJsonText(const std::string& text) {
  Json::Value value;
  Json::CharReaderBuilder builder;
  std::string errors;
  std::istringstream stream(text);
  if (!Json::parseFromStream(builder, stream, &value, &errors)) {
    throw DatabaseError("stored JSON is malformed: " + errors);
  }
  return value;
}

// Round-trip-exact text for a double (std::to_string keeps only 6 decimals).
std::string DoubleText(double value) {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << std::setprecision(17) << value;
  return out.str();
}

Param OptionalText(const std::optional<std::string>& value) {
  return value ? Param(*value) : Param(std::nullopt);
}

Param OptionalMicros(const std::optional<TimePoint>& value) {
  return value ? Param(ToMicros(*value)) : Param(std::nullopt);
}

const std::string& Required(const PgResult& result, std::size_t row, std::size_t column) {
  return result.Value(row, column);
}

std::optional<std::string> Optional(const PgResult& result, std::size_t row, std::size_t column) {
  return result.rows.at(row).at(column);
}

// ---------- row mapping ----------

const std::string& JobColumns() {
  static const std::string columns =
      "id::text, sequence, type, payload::text, metadata::text, priority, status, " +
      EpochMicros("scheduled_at") +
      ", max_attempts, attempt_count, retry_initial_delay_ms, retry_backoff_multiplier::text, "
      "retry_max_delay_ms, timeout_ms, idempotency_key, last_error, version, " +
      EpochMicros("created_at") + ", " + EpochMicros("updated_at") + ", " +
      EpochMicros("finished_at");
  return columns;
}

Job RowToJob(const PgResult& r, std::size_t row) {
  Job job;
  job.id = JobId::Parse(Required(r, row, 0));
  job.sequence = std::stoll(Required(r, row, 1));
  job.type = Required(r, row, 2);
  job.payload = ParseJsonText(Required(r, row, 3));
  job.metadata = ParseJsonText(Required(r, row, 4));
  job.priority = JobPriorityFromInt(std::stoi(Required(r, row, 5)));
  job.state = ParseJobState(Required(r, row, 6));
  job.scheduled_at = FromMicros(Required(r, row, 7));
  job.retry.max_attempts = std::stoi(Required(r, row, 8));
  job.attempt_count = std::stoi(Required(r, row, 9));
  job.retry.initial_delay = std::chrono::milliseconds(std::stoll(Required(r, row, 10)));
  job.retry.backoff_multiplier = std::stod(Required(r, row, 11));
  job.retry.max_delay = std::chrono::milliseconds(std::stoll(Required(r, row, 12)));
  job.timeout = std::chrono::milliseconds(std::stoll(Required(r, row, 13)));
  job.idempotency_key = Optional(r, row, 14);
  job.last_error = Optional(r, row, 15);
  job.version = std::stoll(Required(r, row, 16));
  job.created_at = FromMicros(Required(r, row, 17));
  job.updated_at = FromMicros(Required(r, row, 18));
  if (const auto finished = Optional(r, row, 19)) job.finished_at = FromMicros(*finished);
  return job;
}

const std::string& AttemptColumns() {
  static const std::string columns = "id, job_id::text, attempt_number, status, " +
                                     EpochMicros("started_at") + ", " +
                                     EpochMicros("finished_at") + ", error_message";
  return columns;
}

JobAttempt RowToAttempt(const PgResult& r, std::size_t row) {
  JobAttempt attempt;
  attempt.id = std::stoll(Required(r, row, 0));
  attempt.job_id = JobId::Parse(Required(r, row, 1));
  attempt.attempt_number = std::stoi(Required(r, row, 2));
  attempt.state = ParseAttemptState(Required(r, row, 3));
  attempt.started_at = FromMicros(Required(r, row, 4));
  if (const auto finished = Optional(r, row, 5)) attempt.finished_at = FromMicros(*finished);
  attempt.error_message = Optional(r, row, 6);
  return attempt;
}

const std::string& ResultSelect() {
  static const std::string select =
      "SELECT r.id, r.job_id::text, r.attempt_id, a.attempt_number, r.success, r.output::text, "
      "r.error_message, " +
      EpochMicros("r.completed_at") +
      " FROM job_results r JOIN job_attempts a ON a.id = r.attempt_id ";
  return select;
}

JobResult RowToResult(const PgResult& r, std::size_t row) {
  JobResult result;
  result.id = std::stoll(Required(r, row, 0));
  result.job_id = JobId::Parse(Required(r, row, 1));
  result.attempt_id = std::stoll(Required(r, row, 2));
  result.attempt_number = std::stoi(Required(r, row, 3));
  result.success = Required(r, row, 4) == "t";
  if (const auto output = Optional(r, row, 5)) result.output = ParseJsonText(*output);
  result.error_message = Optional(r, row, 6);
  result.completed_at = FromMicros(Required(r, row, 7));
  return result;
}

// ---------- errors ----------

// Maps database errors that mean something to the caller onto domain errors.
// Must be called from inside a catch block.
[[noreturn]] void TranslateDatabaseError(const DatabaseError& e) {
  const std::string& state = e.sqlstate();
  if (state == "23505") {
    if (e.constraint() == "uq_jobs_idempotency_key") {
      throw ConflictError("idempotency key is already in use");
    }
    throw ConflictError("uniqueness violated by constraint " + e.constraint());
  }
  if (state == "23514") throw ValidationError("rejected by constraint " + e.constraint());
  if (state == "23503") throw NotFoundError("referenced row does not exist (" + e.constraint() + ")");
  if (state == "TF001") throw InvalidTransitionError(e.what());
  if (state == "TF002") throw ValidationError(e.what());
  throw;
}

template <typename F>
auto Translated(F&& body) -> decltype(body()) {
  try {
    return body();
  } catch (const DatabaseError& e) {
    TranslateDatabaseError(e);
  }
}

void RequireId(const JobId& id) {
  if (id.empty()) throw ValidationError("job id is empty");
}

// ---------- shared statements ----------

Job LockJob(PgConnection& connection, const JobId& id) {
  const auto result = connection.Query(
      "SELECT " + JobColumns() + " FROM jobs WHERE id = $1::uuid FOR UPDATE", {id.str()});
  if (result.empty()) throw NotFoundError("job " + id.str() + " does not exist");
  return RowToJob(result, 0);
}

void CheckExpectations(const Job& job, const StateChange& change) {
  if (change.expected_state && job.state != *change.expected_state) {
    throw ConflictError("job is " + std::string(ToString(job.state)) + ", expected " +
                        std::string(ToString(*change.expected_state)));
  }
  if (change.expected_version && job.version != *change.expected_version) {
    throw ConflictError("job version is " + std::to_string(job.version) + ", expected " +
                        std::to_string(*change.expected_version));
  }
}

void InsertEvent(PgConnection& connection, const JobId& id, std::optional<JobState> from,
                 JobState to, const std::optional<std::string>& reason) {
  connection.Query(
      "INSERT INTO job_events (job_id, from_status, to_status, reason) "
      "VALUES ($1::uuid, $2, $3, $4)",
      {id.str(), from ? Param(std::string(ToString(*from))) : Param(std::nullopt),
       std::string(ToString(to)), OptionalText(reason)});
}

// Writes the new state (the database trigger re-validates the transition) and
// the audit event. Caller holds the row lock and the transaction.
Job ApplyTransition(PgConnection& connection, const Job& job, JobState to,
                    const StateChange& change, bool clear_error) {
  const auto result = connection.Query(
      "UPDATE jobs SET status = $2, "
      "scheduled_at = COALESCE(" + TimestampParam(3) + ", scheduled_at), "
      "last_error = CASE WHEN $4::boolean THEN NULL ELSE COALESCE($5, last_error) END, "
      "finished_at = CASE WHEN $2 IN ('COMPLETED', 'FAILED', 'TIMEOUT', 'CANCELLED') "
      "THEN clock_timestamp() ELSE NULL END "
      "WHERE id = $1::uuid RETURNING " + JobColumns(),
      {job.id.str(), std::string(ToString(to)), OptionalMicros(change.scheduled_at),
       std::string(clear_error ? "true" : "false"), OptionalText(change.last_error)});
  InsertEvent(connection, job.id, job.state, to, change.reason);
  return RowToJob(result, 0);
}

bool OutcomeAllows(AttemptState outcome, JobState next) {
  switch (outcome) {
    case AttemptState::kSucceeded: return next == JobState::kCompleted;
    case AttemptState::kFailed: return next == JobState::kRetryWait || next == JobState::kFailed;
    case AttemptState::kTimedOut: return next == JobState::kRetryWait || next == JobState::kTimeout;
    case AttemptState::kCancelled: return next == JobState::kCancelled;
    case AttemptState::kRunning: return false;
  }
  return false;
}

}  // namespace

// ---------- JobRepository ----------

Job JobRepository::CreateJob(const NewJob& job) {
  job.Validate();
  return Translated([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    const auto result = tx->Query(
        "INSERT INTO jobs (type, payload, metadata, priority, status, scheduled_at, "
        "max_attempts, retry_initial_delay_ms, retry_backoff_multiplier, retry_max_delay_ms, "
        "timeout_ms, idempotency_key) "
        "VALUES ($1, $2::jsonb, $3::jsonb, $4::smallint, $5, COALESCE(" + TimestampParam(6) +
            ", now()), $7::integer, $8::integer, $9::double precision, $10::integer, "
            "$11::integer, $12) RETURNING " + JobColumns(),
        {job.type, ToJsonText(job.payload), ToJsonText(job.metadata),
         std::to_string(ToInt(job.priority)), std::string(ToString(job.initial_state)),
         OptionalMicros(job.scheduled_at), std::to_string(job.retry.max_attempts),
         std::to_string(job.retry.initial_delay.count()),
         DoubleText(job.retry.backoff_multiplier), std::to_string(job.retry.max_delay.count()),
         std::to_string(job.timeout.count()), OptionalText(job.idempotency_key)});
    Job created = RowToJob(result, 0);
    InsertEvent(tx.connection(), created.id, std::nullopt, created.state, "job created");
    tx.Commit();
    return created;
  });
}

std::optional<Job> JobRepository::GetJob(const JobId& id) {
  RequireId(id);
  auto lease = pool_.Acquire();
  const auto result =
      lease->Query("SELECT " + JobColumns() + " FROM jobs WHERE id = $1::uuid", {id.str()});
  if (result.empty()) return std::nullopt;
  return RowToJob(result, 0);
}

std::optional<Job> JobRepository::FindJobByIdempotencyKey(std::string_view key) {
  auto lease = pool_.Acquire();
  const auto result = lease->Query(
      "SELECT " + JobColumns() + " FROM jobs WHERE idempotency_key = $1", {std::string(key)});
  if (result.empty()) return std::nullopt;
  return RowToJob(result, 0);
}

Job JobRepository::UpdateJobState(const JobId& id, JobState to, const StateChange& change) {
  RequireId(id);
  return Translated([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    const Job job = LockJob(tx.connection(), id);
    CheckExpectations(job, change);
    ValidateTransition(job.state, to);
    if (to == JobState::kRunning) {
      throw ValidationError("use CreateAttempt to start a job: it must create the attempt atomically");
    }
    if (job.state == JobState::kRunning && to != JobState::kCancelled) {
      throw ValidationError("use StoreResult to finish a running job: it must close the attempt");
    }
    if (job.state == JobState::kRunning) {  // cancelling: close the active attempt in the same transaction
      tx->Query(
          "UPDATE job_attempts SET status = 'CANCELLED', finished_at = clock_timestamp(), "
          "error_message = $2 WHERE job_id = $1::uuid AND status = 'RUNNING'",
          {id.str(), change.reason.value_or("job cancelled")});
    }
    Job updated = ApplyTransition(tx.connection(), job, to, change, false);
    tx.Commit();
    return updated;
  });
}

Job JobRepository::UpdateJob(const JobId& id, const JobUpdate& update) {
  RequireId(id);
  update.Validate();
  return Translated([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    const Job job = LockJob(tx.connection(), id);
    if (update.expected_version && job.version != *update.expected_version) {
      throw ConflictError("job version is " + std::to_string(job.version) + ", expected " +
                          std::to_string(*update.expected_version));
    }
    if (job.state != JobState::kPending && job.state != JobState::kReady &&
        job.state != JobState::kRetryWait) {
      throw ConflictError("a " + std::string(ToString(job.state)) + " job can no longer be edited");
    }
    if (job.state == JobState::kReady && update.scheduled_at &&
        *update.scheduled_at > std::chrono::system_clock::now()) {
      throw ValidationError("a READY job cannot be scheduled in the future");
    }

    std::vector<std::string> assignments;
    std::vector<Param> params{id.str()};
    const auto add = [&](const std::string& column, const std::string& cast, std::string value) {
      params.emplace_back(std::move(value));
      assignments.push_back(column + " = $" + std::to_string(params.size()) + "::" + cast);
    };
    if (update.payload) add("payload", "jsonb", ToJsonText(*update.payload));
    if (update.metadata) add("metadata", "jsonb", ToJsonText(*update.metadata));
    if (update.priority) add("priority", "smallint", std::to_string(ToInt(*update.priority)));
    if (update.scheduled_at) {
      params.emplace_back(ToMicros(*update.scheduled_at));
      assignments.push_back("scheduled_at = " + TimestampParam(static_cast<int>(params.size())));
    }
    if (update.retry) {
      add("max_attempts", "integer", std::to_string(update.retry->max_attempts));
      add("retry_initial_delay_ms", "integer", std::to_string(update.retry->initial_delay.count()));
      add("retry_backoff_multiplier", "double precision",
          DoubleText(update.retry->backoff_multiplier));
      add("retry_max_delay_ms", "integer", std::to_string(update.retry->max_delay.count()));
    }
    if (update.timeout) add("timeout_ms", "integer", std::to_string(update.timeout->count()));

    std::string sql = "UPDATE jobs SET ";
    for (std::size_t i = 0; i < assignments.size(); ++i) sql += (i ? ", " : "") + assignments[i];
    sql += " WHERE id = $1::uuid RETURNING " + JobColumns();
    const auto result = tx->Query(sql, params);
    Job updated = RowToJob(result, 0);
    tx.Commit();
    return updated;
  });
}

JobAttempt JobRepository::CreateAttempt(const JobId& id) {
  RequireId(id);
  return Translated([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    const Job job = LockJob(tx.connection(), id);
    ValidateTransition(job.state, JobState::kRunning);
    if (job.attempt_count >= job.retry.max_attempts) {
      throw ConflictError("job " + id.str() + " has no attempts left (" +
                          std::to_string(job.retry.max_attempts) + " allowed)");
    }
    const auto counted = tx->Query(
        "UPDATE jobs SET status = 'RUNNING', attempt_count = attempt_count + 1 "
        "WHERE id = $1::uuid RETURNING attempt_count",
        {id.str()});
    const auto inserted = tx->Query(
        "INSERT INTO job_attempts (job_id, attempt_number) VALUES ($1::uuid, $2::integer) "
        "RETURNING " + AttemptColumns(),
        {id.str(), counted.Value(0, 0)});
    InsertEvent(tx.connection(), id, job.state, JobState::kRunning,
                "attempt " + counted.Value(0, 0) + " started");
    tx.Commit();
    return RowToAttempt(inserted, 0);
  });
}

std::vector<JobAttempt> JobRepository::GetAttempts(const JobId& id) {
  RequireId(id);
  auto lease = pool_.Acquire();
  const auto result = lease->Query("SELECT " + AttemptColumns() +
                                       " FROM job_attempts WHERE job_id = $1::uuid "
                                       "ORDER BY attempt_number",
                                   {id.str()});
  std::vector<JobAttempt> attempts;
  attempts.reserve(result.rows.size());
  for (std::size_t i = 0; i < result.rows.size(); ++i) attempts.push_back(RowToAttempt(result, i));
  return attempts;
}

JobResult JobRepository::StoreResult(const JobId& id, int attempt_number,
                                     const AttemptOutcome& outcome, JobState next_state,
                                     const StateChange& change) {
  RequireId(id);
  outcome.Validate();
  if (!OutcomeAllows(outcome.state, next_state)) {
    throw ValidationError("a " + std::string(ToString(outcome.state)) +
                          " attempt cannot move the job to " + std::string(ToString(next_state)));
  }
  return Translated([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    const Job job = LockJob(tx.connection(), id);
    CheckExpectations(job, change);
    ValidateTransition(job.state, next_state);
    if (next_state == JobState::kRetryWait && job.attempt_count >= job.retry.max_attempts) {
      throw ValidationError("no attempts remain, so the job cannot wait for a retry");
    }

    const auto attempt = tx->Query(
        "SELECT id, status FROM job_attempts WHERE job_id = $1::uuid AND attempt_number = $2::integer "
        "FOR UPDATE",
        {id.str(), std::to_string(attempt_number)});
    if (attempt.empty()) {
      throw NotFoundError("job " + id.str() + " has no attempt " + std::to_string(attempt_number));
    }
    if (attempt.Value(0, 1) != "RUNNING") {
      throw ConflictError("attempt " + std::to_string(attempt_number) + " has already finished");
    }

    tx->Query(
        "UPDATE job_attempts SET status = $2, finished_at = clock_timestamp(), error_message = $3 "
        "WHERE id = $1::bigint",
        {attempt.Value(0, 0), std::string(ToString(outcome.state)),
         OptionalText(outcome.error_message)});

    const bool has_output = outcome.output && !outcome.output->isNull();
    tx->Query(
        "INSERT INTO job_results (job_id, attempt_id, success, output, error_message) "
        "VALUES ($1::uuid, $2::bigint, $3::boolean, $4::jsonb, $5)",
        {id.str(), attempt.Value(0, 0), std::string(outcome.success() ? "true" : "false"),
         has_output ? Param(ToJsonText(*outcome.output)) : Param(std::nullopt),
         OptionalText(outcome.error_message)});

    StateChange applied = change;
    if (!applied.last_error && !outcome.success()) applied.last_error = outcome.error_message;
    if (!applied.reason) {
      applied.reason = "attempt " + std::to_string(attempt_number) + " " +
                       std::string(ToString(outcome.state));
    }
    ApplyTransition(tx.connection(), job, next_state, applied, outcome.success());

    const auto stored = tx->Query(ResultSelect() + "WHERE r.attempt_id = $1::bigint",
                                  {attempt.Value(0, 0)});
    tx.Commit();
    return RowToResult(stored, 0);
  });
}

std::optional<JobResult> JobRepository::GetResult(const JobId& id) {
  RequireId(id);
  auto lease = pool_.Acquire();
  const auto result = lease->Query(
      ResultSelect() + "WHERE r.job_id = $1::uuid ORDER BY r.id DESC LIMIT 1", {id.str()});
  if (result.empty()) return std::nullopt;
  return RowToResult(result, 0);
}

std::vector<JobResult> JobRepository::GetResults(const JobId& id) {
  RequireId(id);
  auto lease = pool_.Acquire();
  const auto result =
      lease->Query(ResultSelect() + "WHERE r.job_id = $1::uuid ORDER BY r.id", {id.str()});
  std::vector<JobResult> results;
  results.reserve(result.rows.size());
  for (std::size_t i = 0; i < result.rows.size(); ++i) results.push_back(RowToResult(result, i));
  return results;
}

std::vector<JobEvent> JobRepository::GetEvents(const JobId& id) {
  RequireId(id);
  auto lease = pool_.Acquire();
  const auto result = lease->Query(
      "SELECT id, job_id::text, from_status, to_status, reason, " + EpochMicros("created_at") +
          " FROM job_events WHERE job_id = $1::uuid ORDER BY id",
      {id.str()});
  std::vector<JobEvent> events;
  for (std::size_t i = 0; i < result.rows.size(); ++i) {
    JobEvent event;
    event.id = std::stoll(Required(result, i, 0));
    event.job_id = JobId::Parse(Required(result, i, 1));
    if (const auto from = Optional(result, i, 2)) event.from_state = ParseJobState(*from);
    event.to_state = ParseJobState(Required(result, i, 3));
    event.reason = Optional(result, i, 4);
    event.created_at = FromMicros(Required(result, i, 5));
    events.push_back(std::move(event));
  }
  return events;
}

}  // namespace taskflow
