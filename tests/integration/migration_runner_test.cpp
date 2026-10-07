#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "taskflow/common/errors.hpp"
#include "taskflow/db/migration_runner.hpp"
#include "test_support.hpp"

namespace taskflow {
namespace {

// Every test runs inside its own throwaway schema, so tests never see each
// other's tables and nothing is left behind in the shared database.
class MigrationRunnerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ = testing::IntegrationConfig();
    schema_ = testing::UniqueName("tf_mig");
    admin_ = PgConnection::Connect(config_.postgres);
    admin_->Exec("CREATE SCHEMA " + schema_);
    config_.postgres.options = "-csearch_path=" + schema_;
    pool_ = std::make_unique<PgPool>(config_.postgres);
  }

  void TearDown() override {
    pool_.reset();
    if (admin_) admin_->Exec("DROP SCHEMA IF EXISTS " + schema_ + " CASCADE");
  }

  std::vector<std::int64_t> Apply() {
    return MigrationRunner(*pool_).Run(LoadMigrations(dir_.path()));
  }

  bool TableExists(const std::string& table) {
    return admin_
               ->Query("SELECT count(*) FROM information_schema.tables "
                       "WHERE table_schema = $1 AND table_name = $2",
                       {schema_, table})
               .Value(0, 0) == "1";
  }

  Config config_;
  std::string schema_;
  std::unique_ptr<PgConnection> admin_;
  std::unique_ptr<PgPool> pool_;
  testing::TempDir dir_;
};

TEST_F(MigrationRunnerTest, AppliesPendingMigrationsInOrderAndRecordsThem) {
  dir_.Write("0002_add_column.sql", "ALTER TABLE widgets ADD COLUMN color text;");
  dir_.Write("0001_create_widgets.sql", "CREATE TABLE widgets (id int PRIMARY KEY);");

  const auto applied = Apply();
  EXPECT_EQ(applied, (std::vector<std::int64_t>{1, 2}));
  EXPECT_TRUE(TableExists("widgets"));
  EXPECT_TRUE(TableExists("schema_migrations"));

  const auto history = MigrationRunner(*pool_).Applied();
  ASSERT_EQ(history.size(), 2u);
  EXPECT_EQ(history[0].version, 1);
  EXPECT_EQ(history[0].name, "create_widgets");
  EXPECT_EQ(history[1].version, 2);
  EXPECT_FALSE(history[0].checksum.empty());
}

TEST_F(MigrationRunnerTest, RerunningIsANoOpAndNewMigrationsAreAppliedIncrementally) {
  dir_.Write("0001_create_widgets.sql", "CREATE TABLE widgets (id int);");
  EXPECT_EQ(Apply().size(), 1u);
  EXPECT_TRUE(Apply().empty());  // would fail with "relation already exists" if re-applied

  dir_.Write("0002_create_gadgets.sql", "CREATE TABLE gadgets (id int);");
  EXPECT_EQ(Apply(), (std::vector<std::int64_t>{2}));
  EXPECT_TRUE(TableExists("gadgets"));
}

TEST_F(MigrationRunnerTest, EmptyMigrationSetStillCreatesTheTrackingTable) {
  EXPECT_TRUE(Apply().empty());
  EXPECT_TRUE(TableExists("schema_migrations"));
}

TEST_F(MigrationRunnerTest, EditingAnAppliedMigrationIsDetected) {
  dir_.Write("0001_create_widgets.sql", "CREATE TABLE widgets (id int);");
  Apply();
  dir_.Write("0001_create_widgets.sql", "CREATE TABLE widgets (id bigint);");
  try {
    Apply();
    FAIL() << "expected MigrationError";
  } catch (const MigrationError& e) {
    EXPECT_NE(std::string(e.what()).find("modified"), std::string::npos);
  }
}

TEST_F(MigrationRunnerTest, RemovingAnAppliedMigrationIsDetected) {
  dir_.Write("0001_create_widgets.sql", "CREATE TABLE widgets (id int);");
  Apply();
  std::filesystem::remove(dir_.path() / "0001_create_widgets.sql");
  EXPECT_THROW(Apply(), MigrationError);
}

TEST_F(MigrationRunnerTest, MigrationsOlderThanTheLatestAppliedAreRejected) {
  dir_.Write("0002_second.sql", "CREATE TABLE second (id int);");
  Apply();
  dir_.Write("0001_first.sql", "CREATE TABLE first (id int);");  // added late
  EXPECT_THROW(Apply(), MigrationError);
  EXPECT_FALSE(TableExists("first"));
}

TEST_F(MigrationRunnerTest, FailedMigrationIsRolledBackCompletelyAndNotRecorded) {
  dir_.Write("0001_ok.sql", "CREATE TABLE ok_table (id int);");
  dir_.Write("0002_broken.sql",
             "CREATE TABLE half_done (id int);\n"
             "INSERT INTO table_that_does_not_exist VALUES (1);");
  dir_.Write("0003_never_reached.sql", "CREATE TABLE unreachable (id int);");

  EXPECT_THROW(Apply(), MigrationError);

  EXPECT_TRUE(TableExists("ok_table"));          // earlier migration stays applied
  EXPECT_FALSE(TableExists("half_done"));        // failing migration rolled back as a unit
  EXPECT_FALSE(TableExists("unreachable"));      // later migrations not attempted
  const auto history = MigrationRunner(*pool_).Applied();
  ASSERT_EQ(history.size(), 1u);
  EXPECT_EQ(history[0].version, 1);

  // Fixing the broken migration lets the run continue from where it stopped.
  dir_.Write("0002_broken.sql", "CREATE TABLE half_done (id int);");
  EXPECT_EQ(Apply(), (std::vector<std::int64_t>{2, 3}));
}

TEST_F(MigrationRunnerTest, ConcurrentRunnersApplyEachMigrationExactlyOnce) {
  // Plain CREATE TABLE (no IF NOT EXISTS): a double apply would raise an error.
  dir_.Write("0001_create_widgets.sql", "CREATE TABLE widgets (id int);");
  dir_.Write("0002_create_gadgets.sql", "CREATE TABLE gadgets (id int);");

  std::atomic<int> total_applied{0};
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&] {
      try {
        total_applied += static_cast<int>(Apply().size());
      } catch (...) {
        ++failures;
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(total_applied.load(), 2);
  EXPECT_EQ(MigrationRunner(*pool_).Applied().size(), 2u);
}

}  // namespace
}  // namespace taskflow
