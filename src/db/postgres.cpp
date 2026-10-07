#include "taskflow/db/postgres.hpp"

#include <chrono>
#include <string>
#include <utility>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

struct ResultDeleter {
  void operator()(PGresult* result) const { PQclear(result); }
};
using ResultPtr = std::unique_ptr<PGresult, ResultDeleter>;

std::string Trimmed(std::string text) {
  while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) text.pop_back();
  return text;
}

}  // namespace

const std::string& PgResult::Value(std::size_t row, std::size_t column) const {
  if (row >= rows.size() || column >= rows[row].size()) {
    throw DatabaseError("query result index out of range");
  }
  const auto& cell = rows[row][column];
  if (!cell) throw DatabaseError("query result value is NULL");
  return *cell;
}

std::unique_ptr<PgConnection> PgConnection::Connect(const PostgresConfig& config) {
  const std::string port = std::to_string(config.port);
  const std::string timeout = std::to_string(config.connect_timeout_seconds);
  std::vector<const char*> keys = {"host", "port", "user", "dbname", "connect_timeout",
                                   "application_name"};
  std::vector<const char*> values = {config.host.c_str(),     port.c_str(),    config.user.c_str(),
                                     config.database.c_str(), timeout.c_str(), "taskflow"};
  if (!config.password.empty()) {
    keys.push_back("password");
    values.push_back(config.password.c_str());
  }
  if (!config.options.empty()) {
    keys.push_back("options");
    values.push_back(config.options.c_str());
  }
  keys.push_back(nullptr);
  values.push_back(nullptr);

  PGconn* raw = PQconnectdbParams(keys.data(), values.data(), 0);
  if (raw == nullptr) throw DatabaseError("PostgreSQL connection allocation failed");
  if (PQstatus(raw) != CONNECTION_OK) {
    const std::string reason = Trimmed(PQerrorMessage(raw));
    PQfinish(raw);
    throw DatabaseError("cannot connect to PostgreSQL at " + config.host + ":" +
                        std::to_string(config.port) + ": " + reason);
  }
  std::unique_ptr<PgConnection> connection(new PgConnection(raw));
  // Idempotent DDL ("IF NOT EXISTS") otherwise logs a NOTICE on every start.
  connection->Exec("SET client_min_messages TO warning");
  return connection;
}

PgConnection::~PgConnection() { PQfinish(connection_); }

bool PgConnection::IsHealthy() const { return PQstatus(connection_) == CONNECTION_OK; }

bool PgConnection::EnsureIdle() {
  if (!IsHealthy()) return false;
  if (PQtransactionStatus(connection_) == PQTRANS_IDLE) return true;
  try {
    Exec("ROLLBACK");
  } catch (const Error&) {
    return false;
  }
  return PQtransactionStatus(connection_) == PQTRANS_IDLE;
}

void PgConnection::ThrowLastError(const std::string& context, PGresult* result) const {
  std::string reason = result != nullptr ? Trimmed(PQresultErrorMessage(result)) : "";
  if (reason.empty()) reason = Trimmed(PQerrorMessage(connection_));
  std::string sqlstate;
  std::string constraint;
  if (result != nullptr) {
    if (const char* state = PQresultErrorField(result, PG_DIAG_SQLSTATE)) sqlstate = state;
    if (const char* name = PQresultErrorField(result, PG_DIAG_CONSTRAINT_NAME)) constraint = name;
  }
  throw DatabaseError(context + ": " + reason, std::move(sqlstate), std::move(constraint));
}

void PgConnection::Exec(const std::string& sql) {
  ResultPtr result(PQexec(connection_, sql.c_str()));
  const auto status = result ? PQresultStatus(result.get()) : PGRES_FATAL_ERROR;
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    ThrowLastError("SQL execution failed", result.get());
  }
}

PgResult PgConnection::Query(const std::string& sql, const std::vector<Param>& params) {
  std::vector<const char*> values;
  values.reserve(params.size());
  for (const auto& p : params) values.push_back(p ? p->c_str() : nullptr);

  ResultPtr result(PQexecParams(connection_, sql.c_str(), static_cast<int>(params.size()), nullptr,
                                values.empty() ? nullptr : values.data(), nullptr, nullptr, 0));
  const auto status = result ? PQresultStatus(result.get()) : PGRES_FATAL_ERROR;
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    ThrowLastError("query failed", result.get());
  }

  PgResult out;
  const int row_count = PQntuples(result.get());
  const int column_count = PQnfields(result.get());
  out.rows.reserve(static_cast<std::size_t>(row_count));
  for (int r = 0; r < row_count; ++r) {
    std::vector<std::optional<std::string>> row;
    row.reserve(static_cast<std::size_t>(column_count));
    for (int c = 0; c < column_count; ++c) {
      if (PQgetisnull(result.get(), r, c)) {
        row.emplace_back(std::nullopt);
      } else {
        row.emplace_back(std::string(PQgetvalue(result.get(), r, c),
                                     static_cast<std::size_t>(PQgetlength(result.get(), r, c))));
      }
    }
    out.rows.push_back(std::move(row));
  }
  const char* affected = PQcmdTuples(result.get());
  out.affected_rows = (affected != nullptr && *affected != '\0') ? std::stoull(affected) : 0;
  return out;
}

PgTransaction::PgTransaction(PgConnection& connection) : connection_(connection) {
  connection_.Exec("BEGIN");
}

PgTransaction::~PgTransaction() {
  if (!active_) return;
  try {
    connection_.Exec("ROLLBACK");
  } catch (...) {
    // The pool's EnsureIdle() check discards connections that could not be rolled back.
  }
}

void PgTransaction::Commit() {
  connection_.Exec("COMMIT");
  active_ = false;
}

PgPool::PgPool(const PostgresConfig& config)
    : config_(config),
      pool_(
          config.pool_size, [cfg = config] { return PgConnection::Connect(cfg); },
          [](PgConnection& connection) { return connection.EnsureIdle(); },
          std::chrono::milliseconds(config.acquire_timeout_ms)) {}

PgPool::Lease PgPool::Acquire() { return pool_.Acquire(); }

void PgPool::Ping() {
  auto lease = Acquire();
  try {
    const auto result = lease->Query("SELECT 1");
    if (result.empty() || result.Value(0, 0) != "1") {
      throw DatabaseError("PostgreSQL ping returned an unexpected result");
    }
  } catch (...) {
    lease.MarkBroken();
    throw;
  }
}

}  // namespace taskflow
