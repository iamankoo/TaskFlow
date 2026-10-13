#include <gtest/gtest.h>

#include <filesystem>

#include "schema_fixture.hpp"
#include "taskflow/jobs/job_state.hpp"

namespace taskflow {
namespace {

using testing::SchemaFixture;

const std::string kA = "aaaaaaaa-0000-0000-0000-000000000001";
const std::string kB = "aaaaaaaa-0000-0000-0000-000000000002";
const std::string kC = "aaaaaaaa-0000-0000-0000-000000000003";

class DependencySchemaTest : public SchemaFixture {
 protected:
  void Job(const std::string& id, const std::string& status = "PENDING") {
    Sql("INSERT INTO jobs (id, type, status) VALUES ($1::uuid, 't', $2)", {id, status});
  }
  void Edge(const std::string& job, const std::string& prerequisite) {
    Sql("INSERT INTO job_dependencies (job_id, depends_on_id) VALUES ($1::uuid, $2::uuid)", {job, prerequisite});
  }
  std::string Quote(const std::string& id) { return "'" + id + "'"; }
};

TEST_F(DependencySchemaTest, EdgesNeedExistingJobsOnBothSides) {
  Job(kA);
  ExpectRejected("INSERT INTO job_dependencies VALUES (" + Quote(kA) + ", " + Quote(kB) + ", now())", "23503",
                 "fk_job_dependencies_prerequisite");
  // A missing dependent is caught by the guard trigger first (it must find a PENDING job).
  ExpectRejected("INSERT INTO job_dependencies VALUES (" + Quote(kB) + ", " + Quote(kA) + ", now())", "TF004");
  EXPECT_EQ(Count("job_dependencies"), 0);
}

TEST_F(DependencySchemaTest, SelfDependencyAndDuplicateEdgesAreRejected) {
  Job(kA);
  Job(kB);
  ExpectRejected("INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (" + Quote(kA) + ", " + Quote(kA) + ")",
                 "23514", "ck_job_dependencies_no_self");
  Edge(kB, kA);
  ExpectRejected("INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (" + Quote(kB) + ", " + Quote(kA) + ")",
                 "23505", "pk_job_dependencies");
  EXPECT_NO_THROW(db_->Exec("INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (" + Quote(kA) + ", " +
                            Quote(kB) + ")"))
      << "the opposite direction is a different edge; cycles are the application's check, not a constraint";
}

TEST_F(DependencySchemaTest, EdgesMayOnlyBeAddedToAPendingJobAndAreImmutable) {
  Job(kA);
  Job(kB, "READY");
  ExpectRejected("INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (" + Quote(kB) + ", " + Quote(kA) + ")",
                 "TF004");
  Job(kC);
  Edge(kC, kA);
  ExpectRejected("UPDATE job_dependencies SET depends_on_id = " + Quote(kB), "TF003");
}

TEST_F(DependencySchemaTest, DeletingADependentRemovesItsEdgesButAPrerequisiteCannotBeRemovedFromUnderIt) {
  Job(kA);
  Job(kB);
  Edge(kB, kA);
  ExpectRejected("DELETE FROM jobs WHERE id = " + Quote(kA), "23503", "fk_job_dependencies_prerequisite");
  EXPECT_NO_THROW(db_->Exec("DELETE FROM jobs WHERE id = " + Quote(kB)));  // cascades the edge
  EXPECT_EQ(Count("job_dependencies"), 0);
  EXPECT_NO_THROW(db_->Exec("DELETE FROM jobs WHERE id = " + Quote(kA)));  // nobody waits for it now
}

TEST_F(DependencySchemaTest, AWholeWorkflowCanBeDeletedInOneStatement) {
  Job(kA);
  Job(kB);
  Job(kC);
  Edge(kB, kA);
  Edge(kC, kB);
  EXPECT_NO_THROW(db_->Exec("DELETE FROM jobs"));
  EXPECT_EQ(Count("jobs"), 0);
  EXPECT_EQ(Count("job_dependencies"), 0);
}

TEST_F(DependencySchemaTest, PendingMayFailDirectlyAsTheNewTransitionRequires) {
  Job(kA);
  EXPECT_NO_THROW(db_->Exec("UPDATE jobs SET status = 'FAILED', finished_at = now() WHERE id = " + Quote(kA)));
}

TEST_F(DependencySchemaTest, TheDatabaseRefusesReadyWhilePrerequisitesAreUnfinished) {
  Job(kA);
  Job(kB);
  Job(kC);
  Edge(kC, kA);
  Edge(kC, kB);
  const std::string ready = "UPDATE jobs SET status = 'READY' WHERE id = " + Quote(kC);
  ExpectRejected(ready, "TF004");  // both prerequisites PENDING

  Sql("UPDATE jobs SET status = 'READY' WHERE id = $1::uuid", {kA});
  Sql("UPDATE jobs SET status = 'RUNNING', attempt_count = 1 WHERE id = $1::uuid", {kA});
  Sql("UPDATE jobs SET status = 'COMPLETED', finished_at = now() WHERE id = $1::uuid", {kA});
  ExpectRejected(ready, "TF004");  // one of two prerequisites completed is not enough

  Sql("UPDATE jobs SET status = 'FAILED', finished_at = now() WHERE id = $1::uuid", {kB});
  ExpectRejected(ready, "TF004");  // a failed prerequisite never releases the job

  Sql("UPDATE jobs SET status = 'READY', finished_at = NULL WHERE id = $1::uuid", {kB});  // manual requeue of B
  Sql("UPDATE jobs SET status = 'RUNNING', attempt_count = 1 WHERE id = $1::uuid", {kB});
  ExpectRejected(ready, "TF004");  // still running
  Sql("UPDATE jobs SET status = 'COMPLETED', finished_at = now() WHERE id = $1::uuid", {kB});
  EXPECT_NO_THROW(db_->Exec(ready)) << "all prerequisites COMPLETED";
}

TEST_F(DependencySchemaTest, JobsWithoutPrerequisitesAreUnaffectedByTheReadyRule) {
  Job(kA);
  EXPECT_NO_THROW(db_->Exec("UPDATE jobs SET status = 'READY' WHERE id = " + Quote(kA)));
}

TEST_F(DependencySchemaTest, DependencyLookupsUseTheirIndexes) {
  Sql("INSERT INTO jobs (type) SELECT 't' FROM generate_series(1, 3000)");
  Sql("INSERT INTO job_dependencies (job_id, depends_on_id) "
      "SELECT b.id, a.id FROM (SELECT id, row_number() OVER (ORDER BY sequence) rn FROM jobs) a "
      "JOIN (SELECT id, row_number() OVER (ORDER BY sequence) rn FROM jobs) b ON b.rn = a.rn + 1");
  Sql("ANALYZE job_dependencies");
  db_->Exec("SET enable_seqscan = off");
  const auto plan_of = [&](const std::string& sql) {
    std::string text;
    for (const auto& row : Sql("EXPLAIN " + sql).rows) text += *row[0] + "\n";
    return text;
  };
  const std::string one = Sql("SELECT id::text FROM jobs ORDER BY sequence LIMIT 1 OFFSET 100").Value(0, 0);
  const std::string prerequisites = plan_of("SELECT depends_on_id FROM job_dependencies WHERE job_id = '" + one + "'");
  EXPECT_NE(prerequisites.find("pk_job_dependencies"), std::string::npos) << prerequisites;
  const std::string dependents = plan_of("SELECT job_id FROM job_dependencies WHERE depends_on_id = '" + one + "'");
  EXPECT_NE(dependents.find("idx_job_dependencies_prerequisite"), std::string::npos) << dependents;
}

// ---------- upgrading a database that already ran Phases 1-3 ----------

class DependencyMigrationTest : public SchemaFixture {
 protected:
  DependencyMigrationTest() { apply_migrations_ = false; }

  void Copy(const testing::TempDir& dir, const std::string& name) {
    std::filesystem::copy_file(std::filesystem::path(TASKFLOW_SOURCE_MIGRATIONS_DIR) / name, dir.path() / name,
                               std::filesystem::copy_options::overwrite_existing);
  }
};

TEST_F(DependencyMigrationTest, UpgradingAPhase3DatabaseKeepsItsDataAndAddsTheDependencySchema) {
  testing::TempDir dir;
  for (const char* name : {"0001_create_jobs.sql", "0002_create_job_attempts.sql", "0003_create_job_results.sql",
                           "0004_create_job_events.sql"}) {
    Copy(dir, name);
  }
  MigrationRunner runner(*pool_);
  EXPECT_EQ(runner.Run(LoadMigrations(dir.path())), (std::vector<std::int64_t>{1, 2, 3, 4}));
  EXPECT_FALSE(ExistsRelation("job_dependencies"));

  // Phase 3 behaviour: PENDING -> FAILED is illegal; the table holds real data.
  Sql("INSERT INTO jobs (id, type, status) VALUES ('" + kA + "', 'old.job', 'READY'), ('" + kB +
      "', 'old.job', 'PENDING')");
  Sql("UPDATE jobs SET status = 'RUNNING', attempt_count = 1 WHERE id = '" + kA + "'");
  Sql("INSERT INTO job_attempts (job_id, attempt_number) VALUES ('" + kA + "', 1)");
  Sql("INSERT INTO job_events (job_id, to_status, reason) VALUES ('" + kA + "', 'RUNNING', 'before upgrade')");
  ExpectRejected("UPDATE jobs SET status = 'FAILED', finished_at = now() WHERE id = '" + kB + "'", "TF001");
  const auto before = Sql("SELECT id::text, status, version FROM jobs ORDER BY id");

  Copy(dir, "0005_create_job_dependencies.sql");
  EXPECT_EQ(runner.Run(LoadMigrations(dir.path())), (std::vector<std::int64_t>{5}));  // only the new one

  // Existing rows are untouched.
  const auto after = Sql("SELECT id::text, status, version FROM jobs ORDER BY id");
  ASSERT_EQ(after.rows.size(), before.rows.size());
  for (std::size_t i = 0; i < before.rows.size(); ++i) {
    for (std::size_t c = 0; c < 3; ++c) EXPECT_EQ(after.Value(i, c), before.Value(i, c));
  }
  EXPECT_EQ(Count("job_attempts"), 1);
  EXPECT_EQ(Count("job_events"), 1);

  // The new schema and the replaced trigger are live.
  EXPECT_TRUE(ExistsRelation("job_dependencies"));
  EXPECT_TRUE(ExistsRelation("idx_job_dependencies_prerequisite"));
  EXPECT_NO_THROW(db_->Exec("UPDATE jobs SET status = 'FAILED', finished_at = now() WHERE id = '" + kB + "'"))
      << "PENDING -> FAILED is allowed after the upgrade";
  ExpectRejected("UPDATE jobs SET status = 'READY' WHERE id = '" + kA + "'", "TF001");  // old rules still hold

  // Running everything again changes nothing.
  EXPECT_TRUE(runner.Run(LoadMigrations(dir.path())).empty());
  EXPECT_EQ(runner.Applied().size(), 5u);
}

TEST_F(DependencyMigrationTest, AFreshDatabaseReachesTheSameSchemaInOneRun) {
  const auto applied = MigrationRunner(*pool_).Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
  EXPECT_EQ(applied, (std::vector<std::int64_t>{1, 2, 3, 4, 5}));
  EXPECT_TRUE(ExistsRelation("job_dependencies"));
  Sql("INSERT INTO jobs (id, type) VALUES ('" + kA + "', 't')");
  EXPECT_NO_THROW(db_->Exec("UPDATE jobs SET status = 'FAILED', finished_at = now()"));
}

TEST_F(DependencyMigrationTest, TheReplacedTriggerKeepsEveryPhase2Guarantee) {
  MigrationRunner(*pool_).Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
  Sql("INSERT INTO jobs (id, type, idempotency_key) VALUES ('" + kA + "', 't', 'k')");
  ExpectRejected("UPDATE jobs SET type = 'other'", "TF002");              // immutable columns
  ExpectRejected("UPDATE jobs SET status = 'COMPLETED', finished_at = now()", "TF001");  // illegal edge
  const auto v1 = Sql("SELECT version FROM jobs").Value(0, 0);
  Sql("UPDATE jobs SET priority = 2");
  EXPECT_EQ(std::stoll(Sql("SELECT version FROM jobs").Value(0, 0)), std::stoll(v1) + 1);  // version bump
}

}  // namespace
}  // namespace taskflow
