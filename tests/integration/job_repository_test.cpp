#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "schema_fixture.hpp"
#include "taskflow/common/errors.hpp"
#include "taskflow/db/job_repository.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;
using testing::SchemaFixture;

class JobRepositoryTest : public SchemaFixture {
 protected:
  void SetUp() override {
    SchemaFixture::SetUp();
    repo_ = std::make_unique<JobRepository>(*pool_);
  }

  static NewJob Basic(const std::string& type = "email.send") {
    NewJob job;
    job.type = type;
    return job;
  }

  Job CreateReady(int max_attempts = 3) {
    NewJob job = Basic();
    job.initial_state = JobState::kReady;
    job.retry.max_attempts = max_attempts;
    return repo_->CreateJob(job);
  }

  // A job that has been claimed: READY -> RUNNING with attempt 1.
  std::pair<Job, JobAttempt> CreateRunning(int max_attempts = 3) {
    const Job job = CreateReady(max_attempts);
    const JobAttempt attempt = repo_->CreateAttempt(job.id);
    return {*repo_->GetJob(job.id), attempt};
  }

  static AttemptOutcome Success(Json::Value output = Json::Value(Json::objectValue)) {
    AttemptOutcome outcome;
    outcome.state = AttemptState::kSucceeded;
    outcome.output = std::move(output);
    return outcome;
  }

  static AttemptOutcome Failure(AttemptState state = AttemptState::kFailed,
                                const std::string& message = "boom") {
    AttemptOutcome outcome;
    outcome.state = state;
    outcome.error_message = message;
    return outcome;
  }

  JobState StateOf(const JobId& id) { return repo_->GetJob(id)->state; }

  std::unique_ptr<JobRepository> repo_;
};

// ---------- CreateJob / GetJob ----------

TEST_F(JobRepositoryTest, CreatedJobIsPersistedWithDefaultsAndReadBack) {
  const auto before = std::chrono::system_clock::now() - 2s;
  const Job created = repo_->CreateJob(Basic());

  EXPECT_FALSE(created.id.empty());
  EXPECT_EQ(created.type, "email.send");
  EXPECT_EQ(created.state, JobState::kPending);
  EXPECT_EQ(created.priority, JobPriority::kNormal);
  EXPECT_EQ(created.attempt_count, 0);
  EXPECT_EQ(created.version, 1);
  EXPECT_GT(created.sequence, 0);
  EXPECT_EQ(created.retry, RetryPolicy{});
  EXPECT_EQ(created.timeout, 300000ms);
  EXPECT_FALSE(created.idempotency_key);
  EXPECT_FALSE(created.last_error);
  EXPECT_FALSE(created.finished_at);
  EXPECT_GT(created.created_at, before);
  EXPECT_GE(created.updated_at, created.created_at);
  EXPECT_GE(created.scheduled_at, before);

  const auto fetched = repo_->GetJob(created.id);
  ASSERT_TRUE(fetched);
  EXPECT_EQ(fetched->id, created.id);
  EXPECT_EQ(fetched->created_at, created.created_at);
  EXPECT_EQ(fetched->payload, created.payload);
}

TEST_F(JobRepositoryTest, EveryFieldSurvivesAPersistenceRoundTrip) {
  NewJob job = Basic("billing/invoice:v2");
  job.payload["customer"]["id"] = 42;
  job.payload["tags"][0] = "a";
  job.payload["tags"][1] = "ünïcödé \"quoted\" \n newline";
  job.payload["ratio"] = 0.1;
  job.payload["flag"] = true;
  job.payload["nothing"] = Json::Value(Json::nullValue);
  job.metadata["team"] = "payments";
  job.priority = JobPriority::kCritical;
  job.scheduled_at = TimePoint(std::chrono::microseconds(1893456000123456));  // microsecond precision
  job.retry = {7, 250ms, 1.1, 90s};
  job.timeout = 45s;
  job.idempotency_key = "invoice-2026-10-07";

  const Job created = repo_->CreateJob(job);
  const Job fetched = *repo_->GetJob(created.id);

  EXPECT_EQ(fetched.type, job.type);
  EXPECT_EQ(fetched.payload, job.payload);
  EXPECT_EQ(fetched.metadata, job.metadata);
  EXPECT_EQ(fetched.priority, JobPriority::kCritical);
  EXPECT_EQ(fetched.scheduled_at, *job.scheduled_at);
  EXPECT_EQ(fetched.retry, job.retry);  // includes the exact double 1.1
  EXPECT_EQ(fetched.timeout, 45s);
  EXPECT_EQ(fetched.idempotency_key, "invoice-2026-10-07");
  EXPECT_EQ(fetched.state, JobState::kPending);
}

TEST_F(JobRepositoryTest, ReadyJobsAreCreatedReadyAndGetACreationEvent) {
  const Job job = CreateReady();
  EXPECT_EQ(job.state, JobState::kReady);
  const auto events = repo_->GetEvents(job.id);
  ASSERT_EQ(events.size(), 1u);
  EXPECT_FALSE(events[0].from_state);
  EXPECT_EQ(events[0].to_state, JobState::kReady);
  EXPECT_EQ(events[0].reason, "job created");
}

TEST_F(JobRepositoryTest, InvalidJobsAreRejectedBeforeTouchingTheDatabase) {
  NewJob job = Basic();
  job.type = "";
  EXPECT_THROW(repo_->CreateJob(job), ValidationError);
  job = Basic();
  job.retry.max_attempts = 0;
  EXPECT_THROW(repo_->CreateJob(job), ValidationError);
  job = Basic();
  job.initial_state = JobState::kRunning;
  EXPECT_THROW(repo_->CreateJob(job), ValidationError);
  EXPECT_EQ(Count("jobs"), 0);
  EXPECT_EQ(Count("job_events"), 0);
}

TEST_F(JobRepositoryTest, UnknownAndEmptyIdsAreHandled) {
  EXPECT_FALSE(repo_->GetJob(JobId::Parse("99999999-9999-9999-9999-999999999999")));
  EXPECT_THROW(repo_->GetJob(JobId{}), ValidationError);
  EXPECT_THROW(repo_->UpdateJobState(JobId::Parse("99999999-9999-9999-9999-999999999999"),
                                     JobState::kCancelled),
               NotFoundError);
  EXPECT_THROW(repo_->CreateAttempt(JobId::Parse("99999999-9999-9999-9999-999999999999")),
               NotFoundError);
}

TEST_F(JobRepositoryTest, SequenceIncreasesInSubmissionOrder) {
  const Job a = repo_->CreateJob(Basic());
  const Job b = repo_->CreateJob(Basic());
  const Job c = repo_->CreateJob(Basic());
  EXPECT_LT(a.sequence, b.sequence);
  EXPECT_LT(b.sequence, c.sequence);
}

// ---------- idempotency foundation ----------

TEST_F(JobRepositoryTest, DuplicateIdempotencyKeyIsRejectedAndLeavesOneJob) {
  NewJob job = Basic();
  job.idempotency_key = "order-1";
  const Job first = repo_->CreateJob(job);

  job.type = "different.type";
  EXPECT_THROW(repo_->CreateJob(job), ConflictError);
  EXPECT_EQ(Count("jobs"), 1);
  EXPECT_EQ(Count("job_events"), 1);  // the failed attempt's event was rolled back too

  const auto found = repo_->FindJobByIdempotencyKey("order-1");
  ASSERT_TRUE(found);
  EXPECT_EQ(found->id, first.id);
  EXPECT_EQ(found->type, "email.send");
  EXPECT_FALSE(repo_->FindJobByIdempotencyKey("never-used"));
}

TEST_F(JobRepositoryTest, JobsWithoutKeysNeverConflict) {
  for (int i = 0; i < 5; ++i) EXPECT_NO_THROW(repo_->CreateJob(Basic()));
  EXPECT_EQ(Count("jobs"), 5);
}

TEST_F(JobRepositoryTest, ConcurrentSubmissionsWithOneKeyCreateExactlyOneJob) {
  std::atomic<int> created{0}, conflicts{0}, other{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      NewJob job = Basic();
      job.idempotency_key = "race";
      try {
        repo_->CreateJob(job);
        ++created;
      } catch (const ConflictError&) {
        ++conflicts;
      } catch (...) {
        ++other;
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(created.load(), 1);
  EXPECT_EQ(conflicts.load(), 7);
  EXPECT_EQ(other.load(), 0);
  EXPECT_EQ(Count("jobs"), 1);
}

// ---------- UpdateJobState ----------

TEST_F(JobRepositoryTest, StateChangesArePersistedAndAudited) {
  const Job job = repo_->CreateJob(Basic());
  StateChange change;
  change.reason = "dependencies satisfied";
  const Job ready = repo_->UpdateJobState(job.id, JobState::kReady, change);

  EXPECT_EQ(ready.state, JobState::kReady);
  EXPECT_EQ(ready.version, job.version + 1);
  EXPECT_GE(ready.updated_at, job.updated_at);
  EXPECT_EQ(StateOf(job.id), JobState::kReady);  // read back from the database

  const auto events = repo_->GetEvents(job.id);
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events[1].from_state, JobState::kPending);
  EXPECT_EQ(events[1].to_state, JobState::kReady);
  EXPECT_EQ(events[1].reason, "dependencies satisfied");
}

TEST_F(JobRepositoryTest, IllegalTransitionsAreRejectedAndChangeNothing) {
  const Job job = repo_->CreateJob(Basic());  // PENDING
  const auto version_before = job.version;
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kCompleted), InvalidTransitionError);
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kRetryWait), InvalidTransitionError);
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kPending), InvalidTransitionError);

  const Job after = *repo_->GetJob(job.id);
  EXPECT_EQ(after.state, JobState::kPending);
  EXPECT_EQ(after.version, version_before);
  EXPECT_EQ(repo_->GetEvents(job.id).size(), 1u);
}

TEST_F(JobRepositoryTest, FinishedJobsCannotBeRevived) {
  const Job job = repo_->CreateJob(Basic());
  const Job cancelled = repo_->UpdateJobState(job.id, JobState::kCancelled);
  ASSERT_TRUE(cancelled.finished_at);
  for (const auto to : {JobState::kReady, JobState::kPending, JobState::kRunning,
                        JobState::kCompleted, JobState::kRetryWait}) {
    EXPECT_THROW(repo_->UpdateJobState(job.id, to), Error) << ToString(to);
  }
  EXPECT_EQ(StateOf(job.id), JobState::kCancelled);
}

TEST_F(JobRepositoryTest, RunningCannotBeSetDirectlyBecauseItNeedsAnAttempt) {
  const Job job = CreateReady();
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kRunning), ValidationError);
  EXPECT_EQ(StateOf(job.id), JobState::kReady);
  EXPECT_EQ(Count("job_attempts"), 0);
}

TEST_F(JobRepositoryTest, LeavingRunningNeedsTheAttemptBookkeeping) {
  const auto [job, attempt] = CreateRunning();
  for (const auto to : {JobState::kCompleted, JobState::kFailed, JobState::kRetryWait, JobState::kTimeout}) {
    EXPECT_THROW(repo_->UpdateJobState(job.id, to), ValidationError) << ToString(to);
  }
  EXPECT_EQ(StateOf(job.id), JobState::kRunning);
  EXPECT_EQ(repo_->GetAttempts(job.id)[0].state, AttemptState::kRunning);
}

TEST_F(JobRepositoryTest, CancellingARunningJobClosesItsAttemptAtomically) {
  const auto [job, attempt] = CreateRunning();
  StateChange change;
  change.reason = "cancelled by operator";
  const Job cancelled = repo_->UpdateJobState(job.id, JobState::kCancelled, change);

  EXPECT_EQ(cancelled.state, JobState::kCancelled);
  EXPECT_TRUE(cancelled.finished_at);
  const auto attempts = repo_->GetAttempts(job.id);
  ASSERT_EQ(attempts.size(), 1u);
  EXPECT_EQ(attempts[0].state, AttemptState::kCancelled);
  EXPECT_EQ(attempts[0].error_message, "cancelled by operator");
  EXPECT_TRUE(attempts[0].finished_at);
  EXPECT_FALSE(repo_->GetResult(job.id));  // cancellation is not a result
}

TEST_F(JobRepositoryTest, ExpectedStateAndVersionGuardAgainstStaleWriters) {
  const Job job = repo_->CreateJob(Basic());
  StateChange wrong_state;
  wrong_state.expected_state = JobState::kReady;
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kCancelled, wrong_state), ConflictError);

  StateChange wrong_version;
  wrong_version.expected_version = job.version + 5;
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kReady, wrong_version), ConflictError);
  EXPECT_EQ(StateOf(job.id), JobState::kPending);

  StateChange right;
  right.expected_state = JobState::kPending;
  right.expected_version = job.version;
  EXPECT_NO_THROW(repo_->UpdateJobState(job.id, JobState::kReady, right));
}

TEST_F(JobRepositoryTest, ConcurrentWritersWithTheSameVersionAllowExactlyOne) {
  const Job job = repo_->CreateJob(Basic());
  std::atomic<int> won{0}, lost{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      StateChange change;
      change.expected_version = job.version;
      try {
        repo_->UpdateJobState(job.id, JobState::kReady, change);
        ++won;
      } catch (const ConflictError&) {
        ++lost;
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(won.load(), 1);
  EXPECT_EQ(lost.load(), 7);
  EXPECT_EQ(repo_->GetEvents(job.id).size(), 2u);  // creation + exactly one transition
}

TEST_F(JobRepositoryTest, FinishedAtFollowsTheState) {
  const Job job = repo_->CreateJob(Basic());
  repo_->UpdateJobState(job.id, JobState::kReady);
  EXPECT_FALSE(repo_->GetJob(job.id)->finished_at);
  // run it to failure, then requeue manually
  repo_->CreateAttempt(job.id);
  repo_->StoreResult(job.id, 1, Failure(), JobState::kFailed);
  const Job failed = *repo_->GetJob(job.id);
  EXPECT_TRUE(failed.finished_at);
  const Job requeued = repo_->UpdateJobState(job.id, JobState::kReady);
  EXPECT_FALSE(requeued.finished_at);
}

TEST_F(JobRepositoryTest, ScheduledAtCanBeSetWhileChangingState) {
  const Job job = repo_->CreateJob(Basic());
  StateChange change;
  change.scheduled_at = TimePoint(std::chrono::microseconds(1893456000000000));
  const Job moved = repo_->UpdateJobState(job.id, JobState::kCancelled, change);
  EXPECT_EQ(moved.scheduled_at, *change.scheduled_at);
}

// ---------- UpdateJob ----------

TEST_F(JobRepositoryTest, EditableFieldsCanBeChangedBeforeTheJobRuns) {
  const Job job = repo_->CreateJob(Basic());
  JobUpdate update;
  update.priority = JobPriority::kHigh;
  update.timeout = 5s;
  update.retry = RetryPolicy{5, 100ms, 3.0, 10s};
  update.payload = Json::Value(Json::objectValue);
  (*update.payload)["changed"] = true;
  update.metadata = Json::Value(Json::objectValue);
  (*update.metadata)["by"] = "test";
  update.scheduled_at = TimePoint(std::chrono::microseconds(1893456000000000));
  update.expected_version = job.version;

  const Job updated = repo_->UpdateJob(job.id, update);
  EXPECT_EQ(updated.priority, JobPriority::kHigh);
  EXPECT_EQ(updated.timeout, 5s);
  EXPECT_EQ(updated.retry, (RetryPolicy{5, 100ms, 3.0, 10s}));
  EXPECT_EQ(updated.payload["changed"], true);
  EXPECT_EQ(updated.metadata["by"], "test");
  EXPECT_EQ(updated.scheduled_at, *update.scheduled_at);
  EXPECT_EQ(updated.version, job.version + 1);
  EXPECT_EQ(updated.type, job.type);  // untouched

  EXPECT_EQ(repo_->GetJob(job.id)->priority, JobPriority::kHigh);
}

TEST_F(JobRepositoryTest, OnlyTheNamedFieldsChange) {
  NewJob job = Basic();
  job.priority = JobPriority::kLow;
  job.timeout = 9s;
  const Job created = repo_->CreateJob(job);
  JobUpdate update;
  update.priority = JobPriority::kCritical;
  const Job updated = repo_->UpdateJob(created.id, update);
  EXPECT_EQ(updated.priority, JobPriority::kCritical);
  EXPECT_EQ(updated.timeout, 9s);
  EXPECT_EQ(updated.retry, created.retry);
  EXPECT_EQ(updated.payload, created.payload);
}

TEST_F(JobRepositoryTest, StaleVersionAndWrongStateBlockEdits) {
  const Job job = repo_->CreateJob(Basic());
  JobUpdate update;
  update.priority = JobPriority::kHigh;
  update.expected_version = job.version + 1;
  EXPECT_THROW(repo_->UpdateJob(job.id, update), ConflictError);
  EXPECT_EQ(repo_->GetJob(job.id)->priority, JobPriority::kNormal);

  const auto [running, attempt] = CreateRunning();
  update.expected_version.reset();
  EXPECT_THROW(repo_->UpdateJob(running.id, update), ConflictError);  // RUNNING is not editable
  repo_->UpdateJobState(job.id, JobState::kCancelled);
  EXPECT_THROW(repo_->UpdateJob(job.id, update), ConflictError);      // neither is CANCELLED
}

TEST_F(JobRepositoryTest, ConstraintViolationsInUpdatesBecomeValidationErrors) {
  const auto [running, attempt] = CreateRunning(3);
  repo_->StoreResult(running.id, 1, Failure(), JobState::kRetryWait);  // attempt_count = 1, RETRY_WAIT
  JobUpdate update;
  update.retry = RetryPolicy{};
  update.retry->max_attempts = 1;  // fine: attempt_count (1) <= 1
  EXPECT_NO_THROW(repo_->UpdateJob(running.id, update));

  // Edit to something the schema forbids but the C++ layer cannot see: fewer attempts than already made.
  const auto [second, second_attempt] = CreateRunning(5);
  repo_->StoreResult(second.id, 1, Failure(), JobState::kRetryWait);
  repo_->UpdateJobState(second.id, JobState::kReady);
  repo_->CreateAttempt(second.id);
  repo_->StoreResult(second.id, 2, Failure(), JobState::kRetryWait);  // attempt_count = 2
  JobUpdate lower;
  lower.retry = RetryPolicy{};
  lower.retry->max_attempts = 1;
  EXPECT_THROW(repo_->UpdateJob(second.id, lower), ValidationError);
  EXPECT_EQ(repo_->GetJob(second.id)->retry.max_attempts, 5);
}

TEST_F(JobRepositoryTest, InvalidUpdatesAreRejected) {
  const Job job = repo_->CreateJob(Basic());
  EXPECT_THROW(repo_->UpdateJob(job.id, JobUpdate{}), ValidationError);
  JobUpdate bad;
  bad.timeout = 0ms;
  EXPECT_THROW(repo_->UpdateJob(job.id, bad), ValidationError);
  JobUpdate future;
  future.scheduled_at = std::chrono::system_clock::now() + 1h;
  const Job ready = CreateReady();
  EXPECT_THROW(repo_->UpdateJob(ready.id, future), ValidationError);  // READY means runnable now
  EXPECT_THROW(repo_->UpdateJob(JobId::Parse("99999999-9999-9999-9999-999999999999"), future),
               NotFoundError);
}

// ---------- attempts ----------

TEST_F(JobRepositoryTest, CreatingAnAttemptStartsTheJobAtomically) {
  const Job job = CreateReady();
  const JobAttempt attempt = repo_->CreateAttempt(job.id);

  EXPECT_EQ(attempt.job_id, job.id);
  EXPECT_EQ(attempt.attempt_number, 1);
  EXPECT_EQ(attempt.state, AttemptState::kRunning);
  EXPECT_FALSE(attempt.finished_at);
  EXPECT_FALSE(attempt.error_message);

  const Job running = *repo_->GetJob(job.id);
  EXPECT_EQ(running.state, JobState::kRunning);
  EXPECT_EQ(running.attempt_count, 1);

  const auto events = repo_->GetEvents(job.id);
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events[1].to_state, JobState::kRunning);
  EXPECT_EQ(events[1].reason, "attempt 1 started");
}

TEST_F(JobRepositoryTest, AttemptsCanOnlyStartFromReady) {
  const Job pending = repo_->CreateJob(Basic());
  EXPECT_THROW(repo_->CreateAttempt(pending.id), InvalidTransitionError);
  const auto [running, attempt] = CreateRunning();
  EXPECT_THROW(repo_->CreateAttempt(running.id), InvalidTransitionError);  // already RUNNING
  EXPECT_EQ(Count("job_attempts"), 1);
}

TEST_F(JobRepositoryTest, AttemptsAreNumberedAndListedInOrder) {
  const Job job = CreateReady(3);
  for (int n = 1; n <= 3; ++n) {
    const JobAttempt attempt = repo_->CreateAttempt(job.id);
    EXPECT_EQ(attempt.attempt_number, n);
    if (n < 3) {
      repo_->StoreResult(job.id, n, Failure(), JobState::kRetryWait);
      repo_->UpdateJobState(job.id, JobState::kReady);
    }
  }
  const auto attempts = repo_->GetAttempts(job.id);
  ASSERT_EQ(attempts.size(), 3u);
  for (int i = 0; i < 3; ++i) EXPECT_EQ(attempts[static_cast<std::size_t>(i)].attempt_number, i + 1);
  EXPECT_EQ(attempts[0].state, AttemptState::kFailed);
  EXPECT_EQ(attempts[2].state, AttemptState::kRunning);
}

TEST_F(JobRepositoryTest, NoAttemptsBeyondTheLimit) {
  const Job job = CreateReady(1);
  repo_->CreateAttempt(job.id);
  repo_->StoreResult(job.id, 1, Failure(), JobState::kFailed);
  repo_->UpdateJobState(job.id, JobState::kReady);  // manual requeue does not grant attempts
  EXPECT_THROW(repo_->CreateAttempt(job.id), ConflictError);
  EXPECT_EQ(StateOf(job.id), JobState::kReady);
  EXPECT_EQ(repo_->GetAttempts(job.id).size(), 1u);

  JobUpdate more;  // granting more attempts is an explicit edit
  more.retry = RetryPolicy{};
  more.retry->max_attempts = 2;
  repo_->UpdateJob(job.id, more);
  EXPECT_EQ(repo_->CreateAttempt(job.id).attempt_number, 2);
}

TEST_F(JobRepositoryTest, ConcurrentWorkersCannotBothClaimAJob) {
  const Job job = CreateReady();
  std::atomic<int> claimed{0}, rejected{0}, other{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      try {
        repo_->CreateAttempt(job.id);
        ++claimed;
      } catch (const InvalidTransitionError&) {
        ++rejected;
      } catch (...) {
        ++other;
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(claimed.load(), 1);
  EXPECT_EQ(rejected.load(), 7);
  EXPECT_EQ(other.load(), 0);
  EXPECT_EQ(Count("job_attempts"), 1);
  EXPECT_EQ(repo_->GetJob(job.id)->attempt_count, 1);
}

// ---------- results ----------

TEST_F(JobRepositoryTest, SuccessfulResultCompletesTheJobAndIsReadableOnItsOwn) {
  const auto [job, attempt] = CreateRunning();
  Json::Value output;
  output["rows"] = 12;
  output["detail"]["ok"] = true;
  const JobResult stored = repo_->StoreResult(job.id, 1, Success(output), JobState::kCompleted);

  EXPECT_TRUE(stored.success);
  EXPECT_EQ(stored.job_id, job.id);
  EXPECT_EQ(stored.attempt_id, attempt.id);
  EXPECT_EQ(stored.attempt_number, 1);
  EXPECT_EQ(stored.output, output);
  EXPECT_FALSE(stored.error_message);

  const Job done = *repo_->GetJob(job.id);
  EXPECT_EQ(done.state, JobState::kCompleted);
  EXPECT_TRUE(done.finished_at);
  EXPECT_FALSE(done.last_error);

  const auto result = repo_->GetResult(job.id);  // independent of the job record
  ASSERT_TRUE(result);
  EXPECT_EQ(result->id, stored.id);
  EXPECT_EQ(result->output, output);
  EXPECT_GE(result->completed_at, done.created_at);

  const auto attempts = repo_->GetAttempts(job.id);
  EXPECT_EQ(attempts[0].state, AttemptState::kSucceeded);
  EXPECT_TRUE(attempts[0].finished_at);
  EXPECT_FALSE(attempts[0].error_message);

  const auto events = repo_->GetEvents(job.id);
  EXPECT_EQ(events.back().to_state, JobState::kCompleted);
}

TEST_F(JobRepositoryTest, FailureResultCarriesTheErrorAndSchedulesARetry) {
  const auto [job, attempt] = CreateRunning(3);
  StateChange change;
  change.scheduled_at = TimePoint(std::chrono::microseconds(1893456000000000));
  const JobResult stored =
      repo_->StoreResult(job.id, 1, Failure(AttemptState::kFailed, "connection refused"),
                         JobState::kRetryWait, change);

  EXPECT_FALSE(stored.success);
  EXPECT_EQ(stored.error_message, "connection refused");
  EXPECT_FALSE(stored.output);

  const Job waiting = *repo_->GetJob(job.id);
  EXPECT_EQ(waiting.state, JobState::kRetryWait);
  EXPECT_EQ(waiting.last_error, "connection refused");
  EXPECT_EQ(waiting.scheduled_at, *change.scheduled_at);
  EXPECT_FALSE(waiting.finished_at);
  EXPECT_EQ(repo_->GetAttempts(job.id)[0].error_message, "connection refused");
}

TEST_F(JobRepositoryTest, ARetryCycleKeepsEveryAttemptAndResult) {
  const Job job = CreateReady(3);
  repo_->CreateAttempt(job.id);
  repo_->StoreResult(job.id, 1, Failure(AttemptState::kFailed, "first"), JobState::kRetryWait);
  repo_->UpdateJobState(job.id, JobState::kReady);
  repo_->CreateAttempt(job.id);
  Json::Value out;
  out["answer"] = 42;
  repo_->StoreResult(job.id, 2, Success(out), JobState::kCompleted);

  const auto results = repo_->GetResults(job.id);
  ASSERT_EQ(results.size(), 2u);
  EXPECT_FALSE(results[0].success);
  EXPECT_EQ(results[0].attempt_number, 1);
  EXPECT_TRUE(results[1].success);
  EXPECT_EQ(results[1].attempt_number, 2);
  EXPECT_EQ(repo_->GetResult(job.id)->attempt_number, 2);  // latest
  EXPECT_FALSE(repo_->GetJob(job.id)->last_error);          // cleared by the success
}

TEST_F(JobRepositoryTest, OutcomesMapOntoTheRightFinalStates) {
  struct Case {
    AttemptState outcome;
    JobState next;
    AttemptState attempt_state;
  };
  for (const Case c : {Case{AttemptState::kFailed, JobState::kFailed, AttemptState::kFailed},
                       Case{AttemptState::kTimedOut, JobState::kTimeout, AttemptState::kTimedOut},
                       Case{AttemptState::kTimedOut, JobState::kRetryWait, AttemptState::kTimedOut},
                       Case{AttemptState::kCancelled, JobState::kCancelled, AttemptState::kCancelled}}) {
    const auto [job, attempt] = CreateRunning(3);
    repo_->StoreResult(job.id, 1, Failure(c.outcome), c.next);
    EXPECT_EQ(StateOf(job.id), c.next) << ToString(c.outcome) << " -> " << ToString(c.next);
    EXPECT_EQ(repo_->GetAttempts(job.id)[0].state, c.attempt_state);
    EXPECT_FALSE(repo_->GetResult(job.id)->success);
  }
}

TEST_F(JobRepositoryTest, ContradictoryOutcomeAndStateAreRejected) {
  const auto [job, attempt] = CreateRunning();
  EXPECT_THROW(repo_->StoreResult(job.id, 1, Success(), JobState::kFailed), ValidationError);
  EXPECT_THROW(repo_->StoreResult(job.id, 1, Failure(), JobState::kCompleted), ValidationError);
  EXPECT_THROW(repo_->StoreResult(job.id, 1, Failure(AttemptState::kFailed), JobState::kTimeout),
               ValidationError);
  EXPECT_THROW(repo_->StoreResult(job.id, 1, Failure(AttemptState::kTimedOut), JobState::kFailed),
               ValidationError);
  EXPECT_EQ(StateOf(job.id), JobState::kRunning);
  EXPECT_EQ(Count("job_results"), 0);
}

TEST_F(JobRepositoryTest, RetryWaitRequiresAnAttemptToRemain) {
  const auto [job, attempt] = CreateRunning(1);
  EXPECT_THROW(repo_->StoreResult(job.id, 1, Failure(), JobState::kRetryWait), ValidationError);
  EXPECT_EQ(StateOf(job.id), JobState::kRunning);
  EXPECT_NO_THROW(repo_->StoreResult(job.id, 1, Failure(), JobState::kFailed));
}

TEST_F(JobRepositoryTest, ResultsRequireARunningJobAndRunningAttempt) {
  const Job ready = CreateReady();
  EXPECT_THROW(repo_->StoreResult(ready.id, 1, Success(), JobState::kCompleted), InvalidTransitionError);

  const auto [job, attempt] = CreateRunning();
  EXPECT_THROW(repo_->StoreResult(job.id, 7, Success(), JobState::kCompleted), NotFoundError);
  repo_->StoreResult(job.id, 1, Success(), JobState::kCompleted);
  // A duplicate report for the same attempt is refused: the job is already COMPLETED.
  EXPECT_THROW(repo_->StoreResult(job.id, 1, Success(), JobState::kCompleted), InvalidTransitionError);
  EXPECT_EQ(Count("job_results"), 1);
}

TEST_F(JobRepositoryTest, InvalidOutcomesAreRejectedBeforeTheDatabase) {
  const auto [job, attempt] = CreateRunning();
  AttemptOutcome bad;
  bad.state = AttemptState::kFailed;  // no error message
  EXPECT_THROW(repo_->StoreResult(job.id, 1, bad, JobState::kFailed), ValidationError);
  bad.state = AttemptState::kRunning;
  EXPECT_THROW(repo_->StoreResult(job.id, 1, bad, JobState::kFailed), ValidationError);
}

TEST_F(JobRepositoryTest, JobsWithoutResultsHaveNone) {
  const Job job = repo_->CreateJob(Basic());
  EXPECT_FALSE(repo_->GetResult(job.id));
  EXPECT_TRUE(repo_->GetResults(job.id).empty());
  EXPECT_TRUE(repo_->GetAttempts(job.id).empty());
}

// ---------- transactional consistency ----------

// Installs a trigger that makes a chosen kind of write fail, to prove that the
// writes made earlier in the same transaction are rolled back.
class RollbackTest : public JobRepositoryTest {
 protected:
  void InjectFailure(const std::string& table, const std::string& when) {
    Sql("CREATE FUNCTION tf_inject_failure() RETURNS trigger LANGUAGE plpgsql AS "
        "$$ BEGIN RAISE EXCEPTION 'injected failure' USING ERRCODE = 'XX999'; END $$");
    Sql("CREATE TRIGGER tf_inject BEFORE " + when + " ON " + table +
        " FOR EACH ROW EXECUTE FUNCTION tf_inject_failure()");
  }
  void RemoveFailure(const std::string& table) {
    Sql("DROP TRIGGER tf_inject ON " + table);
  }
};

TEST_F(RollbackTest, FailedJobCreationLeavesNoJobBehind) {
  InjectFailure("job_events", "INSERT");  // the event is written after the job row
  EXPECT_THROW(repo_->CreateJob(Basic()), DatabaseError);
  EXPECT_EQ(Count("jobs"), 0);
  EXPECT_EQ(Count("job_events"), 0);

  RemoveFailure("job_events");
  EXPECT_NO_THROW(repo_->CreateJob(Basic()));  // same call succeeds once the fault is gone
  EXPECT_EQ(Count("jobs"), 1);
}

TEST_F(RollbackTest, FailedAttemptCreationDoesNotStartTheJob) {
  const Job job = CreateReady();
  InjectFailure("job_attempts", "INSERT");  // after the job was already set to RUNNING in the transaction
  EXPECT_THROW(repo_->CreateAttempt(job.id), DatabaseError);

  const Job after = *repo_->GetJob(job.id);
  EXPECT_EQ(after.state, JobState::kReady);
  EXPECT_EQ(after.attempt_count, 0);
  EXPECT_EQ(after.version, job.version);
  EXPECT_EQ(Count("job_attempts"), 0);
  EXPECT_EQ(repo_->GetEvents(job.id).size(), 1u);

  RemoveFailure("job_attempts");
  EXPECT_EQ(repo_->CreateAttempt(job.id).attempt_number, 1);
}

TEST_F(RollbackTest, FailedStateChangeRollsBackTheResultAndTheAttemptUpdate) {
  const auto [job, attempt] = CreateRunning();
  // The job transition is the last write of StoreResult; make it fail.
  Sql("CREATE FUNCTION tf_inject_failure() RETURNS trigger LANGUAGE plpgsql AS "
      "$$ BEGIN RAISE EXCEPTION 'injected failure' USING ERRCODE = 'XX999'; END $$");
  Sql("CREATE TRIGGER tf_inject BEFORE UPDATE ON jobs FOR EACH ROW WHEN (NEW.status = 'COMPLETED') "
      "EXECUTE FUNCTION tf_inject_failure()");

  EXPECT_THROW(repo_->StoreResult(job.id, 1, Success(), JobState::kCompleted), DatabaseError);

  EXPECT_EQ(StateOf(job.id), JobState::kRunning);
  EXPECT_EQ(Count("job_results"), 0);                       // the INSERT was rolled back
  EXPECT_EQ(repo_->GetAttempts(job.id)[0].state, AttemptState::kRunning);  // so was the attempt UPDATE
  EXPECT_FALSE(repo_->GetAttempts(job.id)[0].finished_at);
  EXPECT_EQ(repo_->GetEvents(job.id).size(), 2u);           // creation + attempt start only

  RemoveFailure("jobs");
  EXPECT_NO_THROW(repo_->StoreResult(job.id, 1, Success(), JobState::kCompleted));
  EXPECT_EQ(Count("job_results"), 1);
}

TEST_F(RollbackTest, FailedCancellationKeepsTheAttemptRunning) {
  const auto [job, attempt] = CreateRunning();
  Sql("CREATE FUNCTION tf_inject_failure() RETURNS trigger LANGUAGE plpgsql AS "
      "$$ BEGIN RAISE EXCEPTION 'injected failure' USING ERRCODE = 'XX999'; END $$");
  Sql("CREATE TRIGGER tf_inject BEFORE UPDATE ON jobs FOR EACH ROW WHEN (NEW.status = 'CANCELLED') "
      "EXECUTE FUNCTION tf_inject_failure()");
  EXPECT_THROW(repo_->UpdateJobState(job.id, JobState::kCancelled), DatabaseError);
  EXPECT_EQ(StateOf(job.id), JobState::kRunning);
  EXPECT_EQ(repo_->GetAttempts(job.id)[0].state, AttemptState::kRunning);  // attempt not closed alone
}

TEST_F(RollbackTest, ThePoolIsHealthyAfterRolledBackTransactions) {
  InjectFailure("job_events", "INSERT");
  for (int i = 0; i < 20; ++i) EXPECT_THROW(repo_->CreateJob(Basic()), DatabaseError);
  RemoveFailure("job_events");
  EXPECT_NO_THROW(repo_->CreateJob(Basic()));
  EXPECT_EQ(Count("jobs"), 1);
}

TEST_F(JobRepositoryTest, ATransactionLeftOpenIsRolledBackBeforeTheConnectionIsReused) {
  Config config = config_;
  config.postgres.pool_size = 1;
  PgPool single(config.postgres);
  {
    auto lease = single.Acquire();
    lease->Exec("BEGIN");
    lease->Query("INSERT INTO jobs (type) VALUES ('never committed')");
  }  // returned to the pool with the transaction still open
  auto lease = single.Acquire();  // the same physical connection
  EXPECT_EQ(lease->Query("SELECT count(*) FROM jobs").Value(0, 0), "0");
  EXPECT_NO_THROW(lease->Query("INSERT INTO jobs (type) VALUES ('fresh')"));
}

TEST_F(JobRepositoryTest, PgTransactionCommitsOnlyWhenToldTo) {
  {
    auto lease = pool_->Acquire();
    PgTransaction tx(*lease);
    tx->Query("INSERT INTO jobs (type) VALUES ('rolled back')");
  }  // no Commit()
  EXPECT_EQ(Count("jobs"), 0);
  {
    auto lease = pool_->Acquire();
    PgTransaction tx(*lease);
    tx->Query("INSERT INTO jobs (type) VALUES ('committed')");
    tx.Commit();
  }
  EXPECT_EQ(Count("jobs"), 1);
  try {
    auto lease = pool_->Acquire();
    PgTransaction tx(*lease);
    tx->Query("INSERT INTO jobs (type) VALUES ('also rolled back')");
    throw std::runtime_error("something failed mid-transaction");
  } catch (const std::runtime_error&) {
  }
  EXPECT_EQ(Count("jobs"), 1);
}

// ---------- persistence through a full lifecycle ----------

TEST_F(JobRepositoryTest, FullLifecycleIsDurableAndAuditable) {
  // Create -> persist -> read -> state updates -> attempts -> result.
  NewJob job = Basic("report.generate");
  job.payload["month"] = "2026-09";
  job.idempotency_key = "report-2026-09";
  const Job created = repo_->CreateJob(job);

  StateChange due;
  due.reason = "due";
  repo_->UpdateJobState(created.id, JobState::kReady, due);
  const JobAttempt attempt = repo_->CreateAttempt(created.id);
  Json::Value output;
  output["file"] = "report.pdf";
  repo_->StoreResult(created.id, attempt.attempt_number, Success(output), JobState::kCompleted);

  // A brand-new repository over a brand-new pool sees exactly the same durable state.
  PgPool fresh_pool(config_.postgres);
  JobRepository fresh(fresh_pool);
  const Job loaded = *fresh.GetJob(created.id);
  EXPECT_EQ(loaded.state, JobState::kCompleted);
  EXPECT_EQ(loaded.payload["month"], "2026-09");
  EXPECT_EQ(loaded.attempt_count, 1);
  EXPECT_EQ(fresh.GetResult(created.id)->output, output);
  EXPECT_EQ(fresh.GetAttempts(created.id).size(), 1u);

  const auto events = fresh.GetEvents(created.id);
  ASSERT_EQ(events.size(), 4u);
  const JobState expected[] = {JobState::kPending, JobState::kReady, JobState::kRunning, JobState::kCompleted};
  for (std::size_t i = 0; i < events.size(); ++i) EXPECT_EQ(events[i].to_state, expected[i]);
  for (std::size_t i = 1; i < events.size(); ++i) EXPECT_EQ(events[i].from_state, expected[i - 1]);
}

}  // namespace
}  // namespace taskflow
