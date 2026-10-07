#pragma once

#include <libpq-fe.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "taskflow/common/resource_pool.hpp"
#include "taskflow/config/config.hpp"

namespace taskflow {

// Rows returned by a query. Every value is text; SQL NULL is std::nullopt.
struct PgResult {
  std::vector<std::vector<std::optional<std::string>>> rows;
  std::size_t affected_rows = 0;

  bool empty() const { return rows.empty(); }
  // Returns the value at (row, column) or throws DatabaseError if out of range or NULL.
  const std::string& Value(std::size_t row, std::size_t column) const;
};

// Thin RAII wrapper around one libpq connection. Not thread-safe: one thread
// at a time, which PgPool guarantees through leases.
class PgConnection {
 public:
  // Throws DatabaseError if the server cannot be reached or rejects the login.
  static std::unique_ptr<PgConnection> Connect(const PostgresConfig& config);

  PgConnection(const PgConnection&) = delete;
  PgConnection& operator=(const PgConnection&) = delete;
  ~PgConnection();

  bool IsHealthy() const;

  // Pool hygiene: a connection returned with a transaction still open (for
  // example after a failed ROLLBACK) must not be handed to the next caller.
  // Rolls back any open transaction; returns false if the connection is unusable.
  bool EnsureIdle();

  // Runs raw SQL over the simple-query protocol. May contain several statements.
  void Exec(const std::string& sql);

  // A statement parameter: text, or std::nullopt for SQL NULL.
  using Param = std::optional<std::string>;

  // Runs a single parameterized statement ($1, $2, ...). Throws DatabaseError on failure.
  PgResult Query(const std::string& sql, const std::vector<Param>& params = {});

 private:
  explicit PgConnection(PGconn* connection) : connection_(connection) {}
  [[noreturn]] void ThrowLastError(const std::string& context, PGresult* result) const;

  PGconn* connection_;
};

// RAII transaction. Rolls back on destruction unless Commit() succeeded, so an
// exception thrown anywhere inside the scope can never leave partial changes.
class PgTransaction {
 public:
  explicit PgTransaction(PgConnection& connection);
  ~PgTransaction();
  PgTransaction(const PgTransaction&) = delete;
  PgTransaction& operator=(const PgTransaction&) = delete;

  PgConnection& connection() { return connection_; }
  PgConnection* operator->() { return &connection_; }

  void Commit();

 private:
  PgConnection& connection_;
  bool active_ = true;
};

class PgPool {
 public:
  using Lease = ResourcePool<PgConnection>::Lease;

  explicit PgPool(const PostgresConfig& config);

  // Blocks up to postgres.acquire_timeout_ms. Throws PoolTimeoutError or DatabaseError.
  Lease Acquire();

  // Cheap round-trip (SELECT 1). Throws DatabaseError if PostgreSQL is unreachable.
  void Ping();

  const PostgresConfig& config() const { return config_; }

 private:
  PostgresConfig config_;
  ResourcePool<PgConnection> pool_;
};

}  // namespace taskflow
