#include "taskflow/db/migration_runner.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <fstream>
#include <map>
#include <regex>
#include <sstream>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

// Arbitrary constant identifying "TaskFlow migration runner" to pg_advisory_lock.
constexpr const char* kAdvisoryLockKey = "727274";

constexpr const char* kCreateTrackingTable =
    "CREATE TABLE IF NOT EXISTS schema_migrations ("
    " version BIGINT PRIMARY KEY,"
    " name TEXT NOT NULL,"
    " checksum TEXT NOT NULL,"
    " applied_at TIMESTAMPTZ NOT NULL DEFAULT now())";

std::string NormalizeLineEndings(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (std::size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '\r') {
      if (i + 1 < text.size() && text[i + 1] == '\n') continue;  // CRLF -> LF
      out += '\n';                                                 // lone CR -> LF
    } else {
      out += text[i];
    }
  }
  return out;
}

std::string MigrationLabel(const Migration& m) {
  char prefix[32];
  std::snprintf(prefix, sizeof(prefix), "%04lld_", static_cast<long long>(m.version));
  return prefix + m.name;
}

std::vector<AppliedMigration> ReadApplied(PgConnection& connection) {
  std::vector<AppliedMigration> applied;
  const auto result = connection.Query(
      "SELECT version, name, checksum, applied_at::text FROM schema_migrations ORDER BY version");
  for (std::size_t i = 0; i < result.rows.size(); ++i) {
    applied.push_back({std::stoll(result.Value(i, 0)), result.Value(i, 1), result.Value(i, 2),
                       result.Value(i, 3)});
  }
  return applied;
}

// Holds the session-level advisory lock for the lifetime of a run.
class AdvisoryLock {
 public:
  explicit AdvisoryLock(PgPool::Lease& lease) : lease_(lease) {
    lease_->Query(std::string("SELECT pg_advisory_lock(") + kAdvisoryLockKey + ")");
  }
  ~AdvisoryLock() {
    try {
      lease_->Query(std::string("SELECT pg_advisory_unlock(") + kAdvisoryLockKey + ")");
    } catch (...) {
      lease_.MarkBroken();  // closing the session releases the lock
    }
  }
  AdvisoryLock(const AdvisoryLock&) = delete;
  AdvisoryLock& operator=(const AdvisoryLock&) = delete;

 private:
  PgPool::Lease& lease_;
};

}  // namespace

std::string MigrationChecksum(std::string_view sql) {
  const std::string normalized = NormalizeLineEndings(sql);
  std::uint64_t hash = 14695981039346656037ULL;  // FNV-1a 64-bit
  for (const char c : normalized) {
    hash ^= static_cast<unsigned char>(c);
    hash *= 1099511628211ULL;
  }
  char buffer[17];
  std::snprintf(buffer, sizeof(buffer), "%016llx", static_cast<unsigned long long>(hash));
  return buffer;
}

std::vector<Migration> LoadMigrations(const std::filesystem::path& directory) {
  std::error_code ec;
  if (!std::filesystem::is_directory(directory, ec)) {
    throw MigrationError("migrations directory '" + directory.string() + "' does not exist");
  }

  static const std::regex kFilePattern(R"(^(\d{4,})_([a-z0-9_]+)\.sql$)");
  std::map<std::int64_t, Migration> by_version;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (!entry.is_regular_file() || entry.path().extension() != ".sql") continue;
    const std::string filename = entry.path().filename().string();
    std::smatch match;
    if (!std::regex_match(filename, match, kFilePattern)) {
      throw MigrationError("migration file '" + filename +
                           "' must be named NNNN_lowercase_name.sql");
    }
    Migration migration;
    migration.version = std::stoll(match[1].str());
    migration.name = match[2].str();

    std::ifstream in(entry.path(), std::ios::binary);
    if (!in) throw MigrationError("cannot read migration file '" + filename + "'");
    std::ostringstream buffer;
    buffer << in.rdbuf();
    migration.sql = NormalizeLineEndings(buffer.str());
    migration.checksum = MigrationChecksum(migration.sql);

    if (!by_version.emplace(migration.version, migration).second) {
      throw MigrationError("duplicate migration version " + match[1].str());
    }
  }

  std::vector<Migration> sorted;
  sorted.reserve(by_version.size());
  for (auto& [version, migration] : by_version) sorted.push_back(std::move(migration));
  return sorted;
}

std::vector<AppliedMigration> MigrationRunner::Applied() {
  auto lease = pool_.Acquire();
  lease->Exec(kCreateTrackingTable);
  return ReadApplied(*lease);
}

std::vector<std::int64_t> MigrationRunner::Run(const std::vector<Migration>& migrations) {
  for (std::size_t i = 1; i < migrations.size(); ++i) {
    if (migrations[i].version <= migrations[i - 1].version) {
      throw MigrationError("migrations must be sorted by unique, increasing version");
    }
  }

  auto lease = pool_.Acquire();
  std::vector<std::int64_t> newly_applied;
  try {
    AdvisoryLock lock(lease);
    lease->Exec(kCreateTrackingTable);

    const auto applied = ReadApplied(*lease);
    std::map<std::int64_t, const Migration*> known;
    for (const auto& m : migrations) known[m.version] = &m;

    std::int64_t highest_applied = 0;
    for (const auto& a : applied) {
      const auto it = known.find(a.version);
      if (it == known.end()) {
        throw MigrationError("applied migration " + std::to_string(a.version) + "_" + a.name +
                             " is missing from the migrations directory");
      }
      if (it->second->checksum != a.checksum) {
        throw MigrationError("applied migration " + MigrationLabel(*it->second) +
                             " was modified after being applied");
      }
      highest_applied = std::max(highest_applied, a.version);
    }

    std::vector<const Migration*> pending;
    for (const auto& m : migrations) {
      const bool done = std::any_of(applied.begin(), applied.end(),
                                    [&](const AppliedMigration& a) { return a.version == m.version; });
      if (done) continue;
      if (m.version < highest_applied) {
        throw MigrationError("migration " + MigrationLabel(m) +
                             " is older than the latest applied migration " +
                             std::to_string(highest_applied));
      }
      pending.push_back(&m);
    }

    for (const Migration* m : pending) {
      spdlog::info("applying migration {}", MigrationLabel(*m));
      lease->Exec("BEGIN");
      try {
        lease->Exec(m->sql);
        lease->Query("INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
                     {std::to_string(m->version), m->name, m->checksum});
        lease->Exec("COMMIT");
      } catch (const Error& e) {
        try {
          lease->Exec("ROLLBACK");
        } catch (...) {
          lease.MarkBroken();
        }
        throw MigrationError("migration " + MigrationLabel(*m) + " failed: " + e.what());
      }
      newly_applied.push_back(m->version);
    }
  } catch (const MigrationError&) {
    throw;
  } catch (const Error& e) {
    throw MigrationError(std::string("migration run failed: ") + e.what());
  }
  return newly_applied;
}

}  // namespace taskflow
