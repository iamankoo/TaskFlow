#include <gtest/gtest.h>

#include <regex>

#include "schema_fixture.hpp"
#include "taskflow/jobs/job_state.hpp"

namespace taskflow {
namespace {

using testing::SchemaFixture;

constexpr const char* kJobId = "11111111-1111-1111-1111-111111111111";

// ---------- migrations ----------

class SchemaMigrationTest : public SchemaFixture {
 protected:
  SchemaMigrationTest() { apply_migrations_ = false; }
};

TEST_F(SchemaMigrationTest, FreshDatabaseReceivesEveryPhase2Object) {
  auto runner = MigrationRunner(*pool_);
  const auto applied = runner.Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
  EXPECT_EQ(applied, (std::vector<std::int64_t>{1, 2, 3, 4}));

  for (const char* relation :
       {"schema_migrations", "jobs", "job_attempts", "job_results", "job_events",
        "idx_jobs_ready_queue", "idx_jobs_due", "idx_jobs_created_at", "idx_job_results_job",
        "idx_job_events_job", "uq_job_attempts_one_running", "uq_jobs_idempotency_key",
        "uq_jobs_sequence"}) {
    EXPECT_TRUE(ExistsRelation(relation)) << relation;
  }

  const auto history = runner.Applied();
  ASSERT_EQ(history.size(), 4u);
  EXPECT_EQ(history[0].name, "create_jobs");
  EXPECT_EQ(history[1].name, "create_job_attempts");
  EXPECT_EQ(history[2].name, "create_job_results");
  EXPECT_EQ(history[3].name, "create_job_events");
}

TEST_F(SchemaMigrationTest, RunningMigrationsAgainChangesNothing) {
  auto runner = MigrationRunner(*pool_);
  runner.Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
  Sql("INSERT INTO jobs (type) VALUES ('survives')");
  const auto before = runner.Applied();

  EXPECT_TRUE(runner.Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR)).empty());
  EXPECT_TRUE(runner.Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR)).empty());

  const auto after = runner.Applied();
  ASSERT_EQ(after.size(), before.size());
  for (std::size_t i = 0; i < before.size(); ++i) {
    EXPECT_EQ(after[i].checksum, before[i].checksum);
    EXPECT_EQ(after[i].applied_at, before[i].applied_at);
  }
  EXPECT_EQ(Count("jobs"), 1);  // data written between runs is untouched
}

TEST_F(SchemaMigrationTest, ShippedMigrationsAreNamedAndOrderedForwardOnly) {
  const auto migrations = LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR);
  ASSERT_EQ(migrations.size(), 4u);
  for (std::size_t i = 0; i < migrations.size(); ++i) {
    EXPECT_EQ(migrations[i].version, static_cast<std::int64_t>(i + 1));
    // The runner wraps each file in its own transaction, so a file must not open or end one.
    // (PL/pgSQL function bodies legitimately contain BEGIN ... END without a semicolon.)
    static const std::regex kTransactionControl(R"((^|\n)\s*(BEGIN|START TRANSACTION|COMMIT|ROLLBACK)\s*;)",
                                                std::regex::icase);
    EXPECT_FALSE(std::regex_search(migrations[i].sql, kTransactionControl))
        << "migration " << migrations[i].version << " must not manage transactions";
  }
}

// ---------- constraints ----------

TEST_F(SchemaFixture, DefaultsProduceAValidPendingJob) {
  Sql("INSERT INTO jobs (type) VALUES ('t')");
  const auto row = Sql(
      "SELECT status, priority, max_attempts, attempt_count, timeout_ms, version, "
      "payload::text, metadata::text, finished_at IS NULL FROM jobs");
  EXPECT_EQ(row.Value(0, 0), "PENDING");
  EXPECT_EQ(row.Value(0, 1), "1");
  EXPECT_EQ(row.Value(0, 2), "3");
  EXPECT_EQ(row.Value(0, 3), "0");
  EXPECT_EQ(row.Value(0, 4), "300000");
  EXPECT_EQ(row.Value(0, 5), "1");
  EXPECT_EQ(row.Value(0, 6), "{}");
  EXPECT_EQ(row.Value(0, 7), "{}");
  EXPECT_EQ(row.Value(0, 8), "t");
}

TEST_F(SchemaFixture, CheckConstraintsRejectInvalidJobs) {
  ExpectRejected("INSERT INTO jobs (type) VALUES ('')", "23514", "ck_jobs_type");
  ExpectRejected("INSERT INTO jobs (type) VALUES (repeat('x', 256))", "23514", "ck_jobs_type");
  ExpectRejected("INSERT INTO jobs (type) VALUES (NULL)", "23502");
  ExpectRejected("INSERT INTO jobs (type, payload) VALUES ('t', '[]')", "23514", "ck_jobs_payload_object");
  ExpectRejected("INSERT INTO jobs (type, metadata) VALUES ('t', '\"s\"')", "23514", "ck_jobs_metadata_object");
  ExpectRejected("INSERT INTO jobs (type, priority) VALUES ('t', 4)", "23514", "ck_jobs_priority");
  ExpectRejected("INSERT INTO jobs (type, priority) VALUES ('t', -1)", "23514", "ck_jobs_priority");
  // The creation trigger answers first for an unknown status; the CHECK is the second line of defence.
  ExpectRejected("INSERT INTO jobs (type, status) VALUES ('t', 'BOGUS')", "TF001");
  db_->Exec("ALTER TABLE jobs DISABLE TRIGGER trg_jobs_initial_state");
  ExpectRejected("INSERT INTO jobs (type, status) VALUES ('t', 'BOGUS')", "23514", "ck_jobs_status");
  db_->Exec("ALTER TABLE jobs ENABLE TRIGGER trg_jobs_initial_state");
  ExpectRejected("INSERT INTO jobs (type, max_attempts) VALUES ('t', 0)", "23514", "ck_jobs_max_attempts");
  ExpectRejected("INSERT INTO jobs (type, max_attempts) VALUES ('t', 1001)", "23514", "ck_jobs_max_attempts");
  ExpectRejected("INSERT INTO jobs (type, max_attempts, attempt_count) VALUES ('t', 2, 3)", "23514",
                 "ck_jobs_attempt_count");
  ExpectRejected("INSERT INTO jobs (type, attempt_count) VALUES ('t', -1)", "23514", "ck_jobs_attempt_count");
  ExpectRejected("INSERT INTO jobs (type, retry_initial_delay_ms, retry_max_delay_ms) VALUES ('t', 500, 100)",
                 "23514", "ck_jobs_retry_delays");
  ExpectRejected("INSERT INTO jobs (type, retry_backoff_multiplier) VALUES ('t', 0.5)", "23514",
                 "ck_jobs_retry_backoff");
  ExpectRejected("INSERT INTO jobs (type, timeout_ms) VALUES ('t', 0)", "23514", "ck_jobs_timeout");
  ExpectRejected("INSERT INTO jobs (type, idempotency_key) VALUES ('t', '')", "23514", "ck_jobs_idempotency_key");
  ExpectRejected("INSERT INTO jobs (type, version) VALUES ('t', 0)", "23514", "ck_jobs_version");
  ExpectRejected("INSERT INTO jobs (type, created_at, updated_at) VALUES ('t', now(), now() - interval '1 hour')",
                 "23514", "ck_jobs_updated_after_created");
  EXPECT_EQ(Count("jobs"), 0);
}

TEST_F(SchemaFixture, FinishedAtIsSetExactlyForFinalStates) {
  // A PENDING job cannot claim to be finished ...
  ExpectRejected("INSERT INTO jobs (type, finished_at) VALUES ('t', now())", "23514",
                 "ck_jobs_finished_matches_status");
  // ... and a final state requires finished_at.
  Sql("INSERT INTO jobs (id, type, status) VALUES ('" + std::string(kJobId) + "', 't', 'READY')");
  Sql("UPDATE jobs SET status = 'RUNNING', attempt_count = 1");
  ExpectRejected("UPDATE jobs SET status = 'COMPLETED'", "23514", "ck_jobs_finished_matches_status");
  EXPECT_NO_THROW(db_->Exec("UPDATE jobs SET status = 'COMPLETED', finished_at = now()"));
}

TEST_F(SchemaFixture, IdempotencyKeysAreUniqueButOptional) {
  Sql("INSERT INTO jobs (type, idempotency_key) VALUES ('t', 'order-1')");
  ExpectRejected("INSERT INTO jobs (type, idempotency_key) VALUES ('other', 'order-1')", "23505",
                 "uq_jobs_idempotency_key");
  // Jobs without a key never collide.
  Sql("INSERT INTO jobs (type) VALUES ('a')");
  Sql("INSERT INTO jobs (type) VALUES ('b')");
  EXPECT_EQ(Count("jobs"), 3);
}

TEST_F(SchemaFixture, SequenceIsAssignedByTheDatabaseAndCannotBeForced) {
  Sql("INSERT INTO jobs (type) VALUES ('a'), ('b'), ('c')");
  const auto rows = Sql("SELECT sequence FROM jobs ORDER BY created_at, sequence");
  EXPECT_LT(std::stoll(rows.Value(0, 0)), std::stoll(rows.Value(1, 0)));
  EXPECT_LT(std::stoll(rows.Value(1, 0)), std::stoll(rows.Value(2, 0)));
  ExpectRejected("INSERT INTO jobs (type, sequence) VALUES ('t', 99)", "428C9");
}

TEST_F(SchemaFixture, ImmutableColumnsCannotBeChanged) {
  Sql("INSERT INTO jobs (id, type, idempotency_key) VALUES ('" + std::string(kJobId) + "', 't', 'k')");
  ExpectRejected("UPDATE jobs SET id = '22222222-2222-2222-2222-222222222222'", "TF002");
  ExpectRejected("UPDATE jobs SET type = 'other'", "TF002");
  ExpectRejected("UPDATE jobs SET idempotency_key = 'other'", "TF002");
  ExpectRejected("UPDATE jobs SET created_at = created_at - interval '1 day'", "TF002");
  ExpectRejected("UPDATE jobs SET sequence = 7", "428C9");
}

TEST_F(SchemaFixture, EveryUpdateBumpsVersionAndRefreshesUpdatedAt) {
  Sql("INSERT INTO jobs (type) VALUES ('t')");
  const auto first = Sql("SELECT version, updated_at FROM jobs");
  Sql("UPDATE jobs SET priority = 2");
  Sql("UPDATE jobs SET priority = 3");
  const auto later = Sql("SELECT version, updated_at > $1::timestamptz FROM jobs", {first.Value(0, 1)});
  EXPECT_EQ(first.Value(0, 0), "1");
  EXPECT_EQ(later.Value(0, 0), "3");
  EXPECT_EQ(later.Value(0, 1), "t");
}

TEST_F(SchemaFixture, ANewJobMustStartPendingOrReady) {
  for (const char* state : {"RUNNING", "COMPLETED", "FAILED", "RETRY_WAIT", "TIMEOUT", "CANCELLED"}) {
    ExpectRejected(std::string("INSERT INTO jobs (type, status, finished_at) VALUES ('t', '") + state +
                       "', CASE WHEN '" + state + "' IN ('COMPLETED','FAILED','TIMEOUT','CANCELLED') "
                       "THEN now() END)",
                   "TF001");
  }
  EXPECT_NO_THROW(db_->Exec("INSERT INTO jobs (type, status) VALUES ('t', 'READY')"));
}

// The C++ state machine and the database trigger must agree on every pair.
TEST_F(SchemaFixture, DatabaseTransitionGuardMatchesTheCppStateMachine) {
  int checked = 0;
  for (const JobState from : kAllJobStates) {
    for (const JobState to : kAllJobStates) {
      if (from == to) continue;
      // Seed a job directly in `from` (bypassing the creation guard just for test setup).
      db_->Exec("ALTER TABLE jobs DISABLE TRIGGER trg_jobs_initial_state");
      Sql("INSERT INTO jobs (type, status, attempt_count, finished_at) "
          "VALUES ('t', $1, 1, CASE WHEN $1 IN ('COMPLETED','FAILED','TIMEOUT','CANCELLED') THEN now() END)",
          {std::string(ToString(from))});
      db_->Exec("ALTER TABLE jobs ENABLE TRIGGER trg_jobs_initial_state");

      const std::string attempt =
          "UPDATE jobs SET status = '" + std::string(ToString(to)) + "', finished_at = " +
          (IsTerminal(to) ? "now()" : "NULL") + " WHERE id = (SELECT id FROM jobs ORDER BY sequence DESC LIMIT 1)";
      if (CanTransition(from, to)) {
        EXPECT_NO_THROW(db_->Exec(attempt)) << ToString(from) << " -> " << ToString(to);
      } else {
        ExpectRejected(attempt, "TF001");
      }
      ++checked;
    }
  }
  EXPECT_EQ(checked, 56);  // 8 * 7 ordered pairs of distinct states
}

TEST_F(SchemaFixture, RewritingTheSameStateIsNotATransition) {
  Sql("INSERT INTO jobs (type, status) VALUES ('t', 'READY')");
  EXPECT_NO_THROW(db_->Exec("UPDATE jobs SET status = 'READY', priority = 2"));
}

// ---------- attempts, results, events ----------

TEST_F(SchemaFixture, AttemptsRequireAnExistingJobAndUniqueNumbers) {
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" + std::string(kJobId) + "', 1)",
                 "23503", "fk_job_attempts_job");
  Sql("INSERT INTO jobs (id, type, status) VALUES ('" + std::string(kJobId) + "', 't', 'READY')");
  Sql("INSERT INTO job_attempts (job_id, attempt_number, status, finished_at, error_message) "
      "VALUES ('" + std::string(kJobId) + "', 1, 'FAILED', now(), 'x')");
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" + std::string(kJobId) + "', 1)",
                 "23505", "uq_job_attempts_job_number");
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" + std::string(kJobId) + "', 0)",
                 "23514", "ck_job_attempts_number");
}

TEST_F(SchemaFixture, AJobHasAtMostOneRunningAttempt) {
  Sql("INSERT INTO jobs (id, type, status) VALUES ('" + std::string(kJobId) + "', 't', 'READY')");
  Sql("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" + std::string(kJobId) + "', 1)");
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" + std::string(kJobId) + "', 2)",
                 "23505", "uq_job_attempts_one_running");
  // Once the first attempt finishes, the next may start.
  Sql("UPDATE job_attempts SET status = 'SUCCEEDED', finished_at = now()");
  EXPECT_NO_THROW(db_->Exec("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" +
                            std::string(kJobId) + "', 2)"));
}

TEST_F(SchemaFixture, AttemptFinishedAtMustMatchItsStatus) {
  Sql("INSERT INTO jobs (id, type) VALUES ('" + std::string(kJobId) + "', 't')");
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number, status) VALUES ('" +
                     std::string(kJobId) + "', 1, 'SUCCEEDED')",
                 "23514", "ck_job_attempts_finished_matches_status");
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number, finished_at) VALUES ('" +
                     std::string(kJobId) + "', 1, now())",
                 "23514", "ck_job_attempts_finished_matches_status");
  ExpectRejected("INSERT INTO job_attempts (job_id, attempt_number, status, finished_at, error_message) VALUES ('" +
                     std::string(kJobId) + "', 1, 'SUCCEEDED', now(), 'cannot have an error')",
                 "23514", "ck_job_attempts_error_message");
}

TEST_F(SchemaFixture, ResultsAreTiedToTheirAttemptAndJob) {
  const std::string other = "22222222-2222-2222-2222-222222222222";
  Sql("INSERT INTO jobs (id, type) VALUES ('" + std::string(kJobId) + "', 't'), ('" + other + "', 't')");
  Sql("INSERT INTO job_attempts (job_id, attempt_number, status, finished_at) "
      "VALUES ('" + std::string(kJobId) + "', 1, 'SUCCEEDED', now())");
  const std::string attempt_id = Sql("SELECT id FROM job_attempts").Value(0, 0);

  // A result must name the job that owns the attempt.
  ExpectRejected("INSERT INTO job_results (job_id, attempt_id, success) VALUES ('" + other + "', " + attempt_id +
                     ", true)",
                 "23503", "fk_job_results_attempt");
  ExpectRejected("INSERT INTO job_results (job_id, attempt_id, success) VALUES ('" + std::string(kJobId) +
                     "', 999999, true)",
                 "23503");
  EXPECT_NO_THROW(db_->Exec("INSERT INTO job_results (job_id, attempt_id, success, output) VALUES ('" +
                            std::string(kJobId) + "', " + attempt_id + ", true, '{\"ok\":1}')"));
  // One result per attempt.
  ExpectRejected("INSERT INTO job_results (job_id, attempt_id, success) VALUES ('" + std::string(kJobId) + "', " +
                     attempt_id + ", true)",
                 "23505", "uq_job_results_attempt");
}

TEST_F(SchemaFixture, ResultShapeSeparatesSuccessFromFailure) {
  Sql("INSERT INTO jobs (id, type) VALUES ('" + std::string(kJobId) + "', 't')");
  Sql("INSERT INTO job_attempts (job_id, attempt_number, status, finished_at, error_message) "
      "VALUES ('" + std::string(kJobId) + "', 1, 'FAILED', now(), 'x'), ('" + std::string(kJobId) +
      "', 2, 'FAILED', now(), 'x')");
  const std::string a1 = Sql("SELECT id FROM job_attempts WHERE attempt_number = 1").Value(0, 0);
  const std::string prefix = "INSERT INTO job_results (job_id, attempt_id, success, output, error_message) VALUES ('" +
                             std::string(kJobId) + "', " + a1 + ", ";
  ExpectRejected(prefix + "false, NULL, NULL)", "23514", "ck_job_results_shape");            // failure without error
  ExpectRejected(prefix + "false, '{\"a\":1}', 'boom')", "23514", "ck_job_results_shape");   // failure with output
  ExpectRejected(prefix + "true, NULL, 'boom')", "23514", "ck_job_results_shape");           // success with error
  EXPECT_NO_THROW(db_->Exec(prefix + "false, NULL, 'boom')"));
}

TEST_F(SchemaFixture, EventsAreAppendOnlyAndValidated) {
  Sql("INSERT INTO jobs (id, type) VALUES ('" + std::string(kJobId) + "', 't')");
  ExpectRejected("INSERT INTO job_events (job_id, to_status) VALUES ('" + std::string(kJobId) + "', 'NOPE')",
                 "23514", "ck_job_events_to");
  ExpectRejected("INSERT INTO job_events (job_id, from_status, to_status) VALUES ('" + std::string(kJobId) +
                     "', 'READY', 'READY')",
                 "23514", "ck_job_events_moved");
  ExpectRejected("INSERT INTO job_events (job_id, to_status) VALUES ('33333333-3333-3333-3333-333333333333', 'READY')",
                 "23503", "fk_job_events_job");
  Sql("INSERT INTO job_events (job_id, to_status, reason) VALUES ('" + std::string(kJobId) + "', 'PENDING', 'created')");
  ExpectRejected("UPDATE job_events SET reason = 'rewritten history'", "TF003");
}

TEST_F(SchemaFixture, DeletingAJobRemovesItsAttemptsResultsAndEvents) {
  Sql("INSERT INTO jobs (id, type) VALUES ('" + std::string(kJobId) + "', 't')");
  Sql("INSERT INTO job_attempts (job_id, attempt_number, status, finished_at) "
      "VALUES ('" + std::string(kJobId) + "', 1, 'SUCCEEDED', now())");
  Sql("INSERT INTO job_results (job_id, attempt_id, success) SELECT job_id, id, true FROM job_attempts");
  Sql("INSERT INTO job_events (job_id, to_status) VALUES ('" + std::string(kJobId) + "', 'PENDING')");

  Sql("DELETE FROM jobs");
  EXPECT_EQ(Count("job_attempts"), 0);
  EXPECT_EQ(Count("job_results"), 0);
  EXPECT_EQ(Count("job_events"), 0);
}

// ---------- indexes ----------

TEST_F(SchemaFixture, ReadyQueueIndexIsUsedForTheSchedulersDequeueQuery) {
  Sql("INSERT INTO jobs (type, status, priority) SELECT 't', 'READY', i % 4 FROM generate_series(1, 2000) i");
  Sql("INSERT INTO jobs (type) SELECT 't' FROM generate_series(1, 2000) i");
  Sql("ANALYZE jobs");
  db_->Exec("SET enable_seqscan = off");  // tiny tables: ask only whether the index *can* serve the query
  const auto plan = Sql(
      "EXPLAIN SELECT id FROM jobs WHERE status = 'READY' AND scheduled_at <= now() "
      "ORDER BY priority DESC, scheduled_at, sequence LIMIT 10");
  std::string text;
  for (const auto& row : plan.rows) text += *row[0] + "\n";
  EXPECT_NE(text.find("idx_jobs_ready_queue"), std::string::npos) << text;
  EXPECT_EQ(text.find("Sort"), std::string::npos) << "the index should provide the order\n" << text;
}

TEST_F(SchemaFixture, DueJobsIndexServesDelayedAndRetryPromotion) {
  Sql("INSERT INTO jobs (type, status, scheduled_at) SELECT 't', 'PENDING', now() + (i || ' seconds')::interval "
      "FROM generate_series(1, 2000) i");
  Sql("INSERT INTO jobs (type, status) SELECT 't', 'READY' FROM generate_series(1, 2000) i");
  Sql("ANALYZE jobs");
  db_->Exec("SET enable_seqscan = off");
  const auto plan = Sql("EXPLAIN SELECT id FROM jobs WHERE status IN ('PENDING', 'RETRY_WAIT') "
                        "AND scheduled_at <= now() + interval '10 seconds' ORDER BY scheduled_at LIMIT 100");
  std::string text;
  for (const auto& row : plan.rows) text += *row[0] + "\n";
  EXPECT_NE(text.find("idx_jobs_due"), std::string::npos) << text;
}

}  // namespace
}  // namespace taskflow
