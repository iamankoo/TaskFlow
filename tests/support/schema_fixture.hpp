#pragma once

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "taskflow/db/migration_runner.hpp"
#include "taskflow/db/postgres.hpp"
#include "test_support.hpp"

namespace taskflow::testing {

// Creates a throwaway PostgreSQL schema, applies the project's real migrations
// (migrations/ in the source tree) into it, and drops it afterwards. Each test
// gets an isolated, fully migrated database without touching shared state.
class SchemaFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ = IntegrationConfig();
    schema_ = UniqueName("tf_schema");
    auto admin = PgConnection::Connect(config_.postgres);
    admin->Exec("CREATE SCHEMA " + schema_);

    config_.postgres.options = "-csearch_path=" + schema_;
    pool_ = std::make_unique<PgPool>(config_.postgres);
    if (apply_migrations_) {
      MigrationRunner(*pool_).Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
    }
    db_ = PgConnection::Connect(config_.postgres);
  }

  void TearDown() override {
    db_.reset();
    pool_.reset();
    auto admin = PgConnection::Connect(IntegrationConfig().postgres);
    admin->Exec("DROP SCHEMA IF EXISTS " + schema_ + " CASCADE");
  }

  // Convenience wrappers around the test's own raw connection.
  PgResult Sql(const std::string& sql, const std::vector<PgConnection::Param>& params = {}) {
    return db_->Query(sql, params);
  }
  long Count(const std::string& table, const std::string& where = "true") {
    return std::stol(db_->Query("SELECT count(*) FROM " + table + " WHERE " + where).Value(0, 0));
  }
  bool ExistsRelation(const std::string& name) {
    return db_->Query("SELECT to_regclass($1) IS NOT NULL", {schema_ + "." + name}).Value(0, 0) ==
           "t";
  }

  // Runs `sql` and expects PostgreSQL to reject it with the given SQLSTATE (and constraint).
  void ExpectRejected(const std::string& sql, const std::string& sqlstate,
                      const std::string& constraint = "") {
    try {
      db_->Exec(sql);
      ADD_FAILURE() << "statement was accepted but should fail with " << sqlstate << ": " << sql;
    } catch (const DatabaseError& e) {
      EXPECT_EQ(e.sqlstate(), sqlstate) << sql << "\n  " << e.what();
      if (!constraint.empty()) {
        EXPECT_EQ(e.constraint(), constraint) << sql;
      }
    }
  }

  bool apply_migrations_ = true;
  Config config_;
  std::string schema_;
  std::unique_ptr<PgPool> pool_;
  std::unique_ptr<PgConnection> db_;
};

}  // namespace taskflow::testing
