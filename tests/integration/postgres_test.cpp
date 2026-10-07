#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "taskflow/common/errors.hpp"
#include "taskflow/db/postgres.hpp"
#include "test_support.hpp"

namespace taskflow {
namespace {

class PostgresTest : public ::testing::Test {
 protected:
  Config config_ = testing::IntegrationConfig();
};

TEST_F(PostgresTest, PingSucceedsAgainstARealServer) {
  PgPool pool(config_.postgres);
  EXPECT_NO_THROW(pool.Ping());
}

TEST_F(PostgresTest, ParameterizedQueriesReturnTypedTextValues) {
  PgPool pool(config_.postgres);
  auto lease = pool.Acquire();
  const auto result = lease->Query("SELECT $1::int + 1 AS n, $2::text AS s, NULL::text AS missing",
                                   {"41", "hello"});
  ASSERT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.Value(0, 0), "42");
  EXPECT_EQ(result.Value(0, 1), "hello");
  EXPECT_FALSE(result.rows[0][2].has_value());
  EXPECT_THROW(result.Value(0, 2), DatabaseError);  // NULL
  EXPECT_THROW(result.Value(5, 0), DatabaseError);  // out of range
}

TEST_F(PostgresTest, ParametersAreNotInterpretedAsSql) {
  PgPool pool(config_.postgres);
  auto lease = pool.Acquire();
  const std::string hostile = "'; DROP TABLE nothing; --";
  const auto result = lease->Query("SELECT $1::text", {hostile});
  EXPECT_EQ(result.Value(0, 0), hostile);
}

TEST_F(PostgresTest, SqlErrorsBecomeDatabaseErrorsAndTheConnectionStaysUsable) {
  PgPool pool(config_.postgres);
  auto lease = pool.Acquire();
  try {
    lease->Query("SELECT * FROM table_that_does_not_exist");
    FAIL() << "expected DatabaseError";
  } catch (const DatabaseError& e) {
    EXPECT_NE(std::string(e.what()).find("table_that_does_not_exist"), std::string::npos);
  }
  EXPECT_EQ(lease->Query("SELECT 1").Value(0, 0), "1");
  EXPECT_THROW(lease->Exec("THIS IS NOT SQL"), DatabaseError);
  EXPECT_TRUE(lease->IsHealthy());
}

TEST_F(PostgresTest, UnreachableServerFailsFastWithDatabaseError) {
  config_.postgres.port = 1;  // nothing listens here
  config_.postgres.connect_timeout_seconds = 1;
  PgPool pool(config_.postgres);
  EXPECT_THROW(pool.Ping(), DatabaseError);
}

TEST_F(PostgresTest, WrongPasswordIsRejected) {
  config_.postgres.password = "definitely-not-the-password";
  PgPool pool(config_.postgres);
  // Only meaningful when the server enforces passwords (the Compose and CI setups do).
  try {
    pool.Ping();
    GTEST_SKIP() << "server accepts any password (trust authentication)";
  } catch (const DatabaseError&) {
    SUCCEED();
  }
}

TEST_F(PostgresTest, PoolSharesAtMostPoolSizeConnectionsAcrossManyThreads) {
  config_.postgres.pool_size = 3;
  PgPool pool(config_.postgres);

  std::mutex mutex;
  std::set<std::string> backend_pids;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 12; ++t) {
    threads.emplace_back([&] {
      try {
        for (int i = 0; i < 20; ++i) {
          auto lease = pool.Acquire();
          const auto pid = lease->Query("SELECT pg_backend_pid()").Value(0, 0);
          std::lock_guard lock(mutex);
          backend_pids.insert(pid);
        }
      } catch (...) {
        ++failures;
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(failures.load(), 0);
  EXPECT_GE(backend_pids.size(), 1u);
  EXPECT_LE(backend_pids.size(), 3u);
}

TEST_F(PostgresTest, PoolRecoversAfterTheServerKillsAConnection) {
  config_.postgres.pool_size = 1;
  PgPool pool(config_.postgres);
  pool.Ping();  // establishes the single pooled connection

  // Kill that backend from a separate, unpooled session.
  auto victim_pid = [&] {
    auto lease = pool.Acquire();
    return lease->Query("SELECT pg_backend_pid()").Value(0, 0);
  }();
  auto admin = PgConnection::Connect(config_.postgres);
  admin->Query("SELECT pg_terminate_backend($1::int)", {victim_pid});

  // The first use of the dead connection fails and discards it; the next one reconnects.
  bool recovered = false;
  for (int attempt = 0; attempt < 3 && !recovered; ++attempt) {
    try {
      pool.Ping();
      recovered = true;
    } catch (const DatabaseError&) {
    }
  }
  EXPECT_TRUE(recovered);
}

}  // namespace
}  // namespace taskflow
