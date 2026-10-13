#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <sstream>
#include <thread>
#include <vector>

#include "taskflow/app/application.hpp"
#include "taskflow/db/job_repository.hpp"
#include "taskflow/db/migration_runner.hpp"
#include "taskflow/scheduler/schedule_queue.hpp"
#include "taskflow/version.hpp"
#include "test_support.hpp"

// Drogon is a per-process singleton that cannot be restarted, so this binary
// starts exactly one real Application (real PostgreSQL, real Redis, real HTTP
// listener) and every test talks to it over a TCP socket. The shutdown test is
// declared last on purpose: tests in a suite run in declaration order.

namespace taskflow {
namespace {

using namespace std::chrono_literals;

struct HttpResponse {
  int status = 0;
  std::string headers;
  std::string body;
};

// Minimal blocking HTTP/1.0-style client: independent of Drogon's own client.
HttpResponse Send(std::uint16_t port, const std::string& method, const std::string& path) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_GE(fd, 0);
  timeval timeout{10, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(fd);
    ADD_FAILURE() << "cannot connect to 127.0.0.1:" << port;
    return {};
  }

  const std::string request = method + " " + path +
                              " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
                              "Content-Length: 0\r\n\r\n";
  EXPECT_EQ(::send(fd, request.data(), request.size(), 0), static_cast<ssize_t>(request.size()));

  std::string raw;
  char buffer[4096];
  for (ssize_t n; (n = ::recv(fd, buffer, sizeof(buffer), 0)) > 0;) raw.append(buffer, static_cast<std::size_t>(n));
  ::close(fd);

  HttpResponse response;
  const auto header_end = raw.find("\r\n\r\n");
  if (raw.rfind("HTTP/1.1 ", 0) == 0 && header_end != std::string::npos) {
    response.status = std::atoi(raw.c_str() + 9);
    response.headers = raw.substr(0, header_end);
    response.body = raw.substr(header_end + 4);
  }
  return response;
}

Json::Value ParseBody(const HttpResponse& response) {
  Json::Value root;
  Json::CharReaderBuilder builder;
  std::string errors;
  std::istringstream stream(response.body);
  EXPECT_TRUE(Json::parseFromStream(builder, stream, &root, &errors)) << errors << "\n" << response.body;
  return root;
}

class HealthEndpointTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    try {
      config_ = testing::IntegrationConfig();
      const char* port = std::getenv("TASKFLOW_TEST_HTTP_PORT");
      config_.http.host = "127.0.0.1";
      config_.http.port = static_cast<std::uint16_t>(port ? std::atoi(port) : 18080);
      // Isolated schema with the real migrations, and Redis database 15, so the scheduler that
      // runs inside the application works on private data.
      schema_ = testing::UniqueName("tf_http");
      PgConnection::Connect(config_.postgres)->Exec("CREATE SCHEMA " + schema_);
      config_.postgres.options = "-csearch_path=" + schema_;
      config_.redis.database = 15;
      config_.scheduler.enabled = true;
      config_.scheduler.tick_interval_ms = 50;
      config_.scheduler.redis_prefix = testing::UniqueName("tfhttp");
      config_.app.auto_migrate = true;
      config_.app.migrations_dir = TASKFLOW_SOURCE_MIGRATIONS_DIR;
      config_.app.shutdown_grace_ms = 1500;

      app_ = std::make_unique<Application>(config_);
      app_->Initialize();
      server_thread_ = std::thread([] {
        try {
          exit_code_ = app_->Run();
        } catch (const std::exception& e) {
          startup_error_ = std::string("server thread died: ") + e.what();
        }
      });
      if (!app_->WaitUntilServing(10s)) startup_error_ = "HTTP server did not start within 10s";
    } catch (const std::exception& e) {
      startup_error_ = std::string("setup failed: ") + e.what();
    }
  }

  static void TearDownTestSuite() {
    if (app_) app_->RequestShutdown();
    if (server_thread_.joinable()) server_thread_.join();
    app_.reset();
    try {
      auto admin = PgConnection::Connect(testing::IntegrationConfig().postgres);
      admin->Exec("DROP SCHEMA IF EXISTS " + schema_ + " CASCADE");
      Config redis_config = testing::IntegrationConfig();
      redis_config.redis.database = 15;
      RedisClient(redis_config.redis).Execute({"FLUSHDB"});
    } catch (const Error&) {
    }
  }

  // A failure in SetUpTestSuite would make gtest report the tests as skipped,
  // which CTest counts as a pass. Fail every test explicitly instead.
  void SetUp() override { ASSERT_TRUE(startup_error_.empty()) << startup_error_; }

  static HttpResponse Get(const std::string& path) { return Send(config_.http.port, "GET", path); }

  static Config config_;
  static std::string schema_;
  static std::unique_ptr<Application> app_;
  static std::thread server_thread_;
  static int exit_code_;
  static std::string startup_error_;
};

Config HealthEndpointTest::config_;
std::string HealthEndpointTest::schema_;
std::unique_ptr<Application> HealthEndpointTest::app_;
std::thread HealthEndpointTest::server_thread_;
int HealthEndpointTest::exit_code_ = -1;
std::string HealthEndpointTest::startup_error_;

TEST_F(HealthEndpointTest, ReturnsOkWithBothDependenciesUp) {
  const HttpResponse response = Get("/health");
  EXPECT_EQ(response.status, 200);

  const Json::Value json = ParseBody(response);
  EXPECT_EQ(json["status"].asString(), "ok");
  EXPECT_EQ(json["version"].asString(), kVersion);
  EXPECT_GE(json["uptime_seconds"].asInt64(), 0);
  EXPECT_EQ(json["dependencies"]["postgres"]["status"].asString(), "up");
  EXPECT_EQ(json["dependencies"]["redis"]["status"].asString(), "up");
}

TEST_F(HealthEndpointTest, ResponseIsJsonAndNotCacheable) {
  const HttpResponse response = Get("/health");
  EXPECT_NE(response.headers.find("application/json"), std::string::npos) << response.headers;
  EXPECT_NE(response.headers.find("no-store"), std::string::npos) << response.headers;
}

TEST_F(HealthEndpointTest, StartupRanMigrationBookkeeping) {
  auto lease = app_->postgres().Acquire();
  const auto result = lease->Query("SELECT to_regclass('schema_migrations') IS NOT NULL");
  EXPECT_EQ(result.Value(0, 0), "t");
}

TEST_F(HealthEndpointTest, UnknownRoutesReturn404) {
  EXPECT_EQ(Get("/does-not-exist").status, 404);
}

TEST_F(HealthEndpointTest, OnlyGetIsAllowedOnHealth) {
  EXPECT_EQ(Send(config_.http.port, "POST", "/health").status, 405);
}

TEST_F(HealthEndpointTest, HandlesConcurrentRequests) {
  std::vector<std::future<int>> requests;
  for (int i = 0; i < 20; ++i) {
    requests.push_back(std::async(std::launch::async, [] { return Get("/health").status; }));
  }
  for (auto& request : requests) EXPECT_EQ(request.get(), 200);
}

TEST_F(HealthEndpointTest, TheSchedulerRunsInsideTheApplicationAndPromotesDueJobs) {
  ASSERT_NE(app_->scheduler(), nullptr);
  EXPECT_TRUE(app_->scheduler()->running());

  JobRepository jobs(app_->postgres());
  NewJob job;
  job.type = "http.test";
  job.priority = JobPriority::kHigh;
  job.scheduled_at = std::chrono::system_clock::now() - std::chrono::seconds(1);  // already due
  const Job created = jobs.CreateJob(job);

  JobState state = JobState::kPending;
  for (int i = 0; i < 100 && state != JobState::kReady; ++i) {
    std::this_thread::sleep_for(50ms);
    state = jobs.GetJob(created.id)->state;
  }
  EXPECT_EQ(state, JobState::kReady) << "the scheduler thread did not promote the job";

  ScheduleQueue queue(app_->redis(), config_.scheduler.redis_prefix);
  EXPECT_EQ(queue.ReadyCount(JobPriority::kHigh), 1u);
}

TEST_F(HealthEndpointTest, ZzShutdownReportsUnhealthyThenStopsCleanly) {
  app_->RequestShutdown();

  // During the grace period the listener is still up but advertises that it is leaving.
  const HttpResponse draining = Get("/health");
  EXPECT_EQ(draining.status, 503);
  EXPECT_EQ(ParseBody(draining)["status"].asString(), "shutting_down");

  server_thread_.join();
  EXPECT_EQ(exit_code_, 0);
  EXPECT_FALSE(app_->scheduler()->running()) << "the scheduler must stop with the application";
}

}  // namespace
}  // namespace taskflow
