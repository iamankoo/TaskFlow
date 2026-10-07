#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "taskflow/db/postgres.hpp"

namespace taskflow {

// One versioned SQL file, e.g. migrations/0001_create_jobs.sql
struct Migration {
  std::int64_t version = 0;
  std::string name;      // "create_jobs"
  std::string sql;
  std::string checksum;  // of the SQL with line endings normalized
};

struct AppliedMigration {
  std::int64_t version = 0;
  std::string name;
  std::string checksum;
  std::string applied_at;
};

// Checksum used to detect edits to already-applied migrations. This guards
// against accidental changes, not tampering.
std::string MigrationChecksum(std::string_view sql);

// Reads every NNNN_name.sql file in `directory`, sorted by version. Other file
// types (README.md) are ignored. Throws MigrationError on a missing directory,
// a malformed *.sql filename, or duplicate versions.
std::vector<Migration> LoadMigrations(const std::filesystem::path& directory);

// Forward-only migration runner. Migrations are applied in version order, each
// in its own transaction together with its bookkeeping row. A PostgreSQL
// advisory lock makes concurrent runners (several containers starting at once)
// safe. Migration SQL must not contain its own BEGIN/COMMIT.
class MigrationRunner {
 public:
  explicit MigrationRunner(PgPool& pool) : pool_(pool) {}

  // Applies every pending migration and returns the versions it applied.
  // Throws MigrationError if an applied migration was edited or removed, if a
  // new migration is older than one already applied, or if SQL fails.
  std::vector<std::int64_t> Run(const std::vector<Migration>& migrations);

  std::vector<AppliedMigration> Applied();

 private:
  PgPool& pool_;
};

}  // namespace taskflow
