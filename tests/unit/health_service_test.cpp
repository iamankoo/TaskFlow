#include <gtest/gtest.h>

#include <stdexcept>

#include "taskflow/api/health_service.hpp"
#include "taskflow/common/errors.hpp"

namespace taskflow {
namespace {

TEST(HealthServiceTest, ReportsOkWhenEveryDependencyIsUp) {
  HealthService health("1.2.3");
  health.AddProbe("postgres", [] {});
  health.AddProbe("redis", [] {});

  const HealthReport report = health.Check();
  EXPECT_TRUE(report.IsHealthy());
  EXPECT_EQ(report.HttpStatus(), 200);

  const Json::Value json = report.ToJson();
  EXPECT_EQ(json["status"].asString(), "ok");
  EXPECT_EQ(json["version"].asString(), "1.2.3");
  EXPECT_GE(json["uptime_seconds"].asInt64(), 0);
  EXPECT_EQ(json["dependencies"]["postgres"]["status"].asString(), "up");
  EXPECT_EQ(json["dependencies"]["redis"]["status"].asString(), "up");
  EXPECT_FALSE(json["dependencies"]["redis"].isMember("error"));
}

TEST(HealthServiceTest, ReportsDegradedAndIdentifiesTheFailedDependency) {
  HealthService health("1.0.0");
  health.AddProbe("postgres", [] {});
  health.AddProbe("redis", [] { throw RedisError("connect to 10.0.0.5:6379 refused, password=hunter2"); });

  const HealthReport report = health.Check();
  EXPECT_FALSE(report.IsHealthy());
  EXPECT_EQ(report.HttpStatus(), 503);

  const Json::Value json = report.ToJson();
  EXPECT_EQ(json["status"].asString(), "degraded");
  EXPECT_EQ(json["dependencies"]["postgres"]["status"].asString(), "up");
  EXPECT_EQ(json["dependencies"]["redis"]["status"].asString(), "down");
  EXPECT_EQ(json["dependencies"]["redis"]["error"].asString(), "redis_error");
}

TEST(HealthServiceTest, NeverLeaksRawErrorTextToClients) {
  HealthService health("1.0.0");
  health.AddProbe("postgres", [] { throw DatabaseError("FATAL: password authentication failed for user admin"); });

  const std::string body = health.Check().ToJson().toStyledString();
  EXPECT_EQ(body.find("password"), std::string::npos);
  EXPECT_EQ(body.find("admin"), std::string::npos);
}

TEST(HealthServiceTest, UnexpectedExceptionsAreReportedAsInternalErrors) {
  HealthService health("1.0.0");
  health.AddProbe("postgres", [] { throw std::runtime_error("oops"); });

  const Json::Value json = health.Check().ToJson();
  EXPECT_EQ(json["dependencies"]["postgres"]["error"].asString(), "internal_error");
}

TEST(HealthServiceTest, ShuttingDownIsUnhealthyEvenIfDependenciesAreUp) {
  HealthService health("1.0.0");
  health.AddProbe("postgres", [] {});
  health.SetShuttingDown();

  const HealthReport report = health.Check();
  EXPECT_FALSE(report.IsHealthy());
  EXPECT_EQ(report.HttpStatus(), 503);
  EXPECT_EQ(report.ToJson()["status"].asString(), "shutting_down");
}

TEST(HealthServiceTest, WithNoProbesTheServiceIsSimplyAlive) {
  HealthService health("1.0.0");
  const HealthReport report = health.Check();
  EXPECT_TRUE(report.IsHealthy());
  EXPECT_TRUE(report.probes.empty());
}

}  // namespace
}  // namespace taskflow
