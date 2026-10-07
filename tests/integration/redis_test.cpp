#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "taskflow/common/errors.hpp"
#include "taskflow/redis/redis_client.hpp"
#include "test_support.hpp"

namespace taskflow {
namespace {

class RedisTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_ = testing::IntegrationConfig();
    prefix_ = testing::UniqueName("tf_test");
  }

  void TearDown() override {
    // Remove only keys this test created; the Redis instance may be shared.
    try {
      RedisClient client(config_.redis);
      for (const auto& key : keys_) client.Execute({"DEL", key});
    } catch (const Error&) {
    }
  }

  std::string Key(const std::string& name) {
    keys_.push_back(prefix_ + ":" + name);
    return keys_.back();
  }

  Config config_;
  std::string prefix_;
  std::vector<std::string> keys_;
};

TEST_F(RedisTest, PingSucceedsAgainstARealServer) {
  RedisClient client(config_.redis);
  EXPECT_NO_THROW(client.Ping());
}

TEST_F(RedisTest, StringRoundTripAndMissingKeysAreNil) {
  RedisClient client(config_.redis);
  const std::string key = Key("greeting");

  EXPECT_TRUE(client.Execute({"GET", key}).IsNil());

  const auto set = client.Execute({"SET", key, "hello world"});
  EXPECT_EQ(set.type, RedisReply::Type::kStatus);
  EXPECT_EQ(set.text, "OK");

  const auto got = client.Execute({"GET", key});
  EXPECT_EQ(got.type, RedisReply::Type::kString);
  EXPECT_EQ(got.text, "hello world");
}

TEST_F(RedisTest, ValuesAreBinarySafe) {
  RedisClient client(config_.redis);
  const std::string key = Key("binary");
  const std::string value("a\0b\r\nc", 6);
  client.Execute({"SET", key, value});
  EXPECT_EQ(client.Execute({"GET", key}).text, value);
}

TEST_F(RedisTest, IntegerAndArrayReplies) {
  RedisClient client(config_.redis);
  const std::string counter = Key("counter");
  const std::string list = Key("list");

  EXPECT_EQ(client.Execute({"INCR", counter}).integer, 1);
  EXPECT_EQ(client.Execute({"INCRBY", counter, "9"}).integer, 10);

  client.Execute({"RPUSH", list, "a", "b", "c"});
  const auto items = client.Execute({"LRANGE", list, "0", "-1"});
  ASSERT_EQ(items.type, RedisReply::Type::kArray);
  ASSERT_EQ(items.elements.size(), 3u);
  EXPECT_EQ(items.elements[0].text, "a");
  EXPECT_EQ(items.elements[2].text, "c");
}

TEST_F(RedisTest, ErrorRepliesBecomeRedisErrorsAndThePoolKeepsWorking) {
  RedisClient client(config_.redis);
  const std::string key = Key("not_a_number");
  client.Execute({"SET", key, "abc"});

  EXPECT_THROW(client.Execute({"INCR", key}), RedisError);
  EXPECT_THROW(client.Execute({"NOT_A_REAL_COMMAND"}), RedisError);
  EXPECT_THROW(client.Execute({}), RedisError);
  EXPECT_NO_THROW(client.Ping());
}

TEST_F(RedisTest, UnreachableServerFailsWithRedisError) {
  config_.redis.port = 1;  // nothing listens here
  RedisClient client(config_.redis);
  EXPECT_THROW(client.Ping(), RedisError);
}

TEST_F(RedisTest, AuthenticationFailureIsReportedWhenServerHasNoPassword) {
  if (!config_.redis.password.empty()) GTEST_SKIP() << "server requires a password";
  config_.redis.password = "wrong";
  RedisClient client(config_.redis);
  EXPECT_THROW(client.Ping(), RedisError);  // "AUTH called without any password configured"
}

TEST_F(RedisTest, ConcurrentClientsShareAPoolWithoutCorruptingReplies) {
  config_.redis.pool_size = 3;
  RedisClient client(config_.redis);
  const std::string counter = Key("shared_counter");

  constexpr int kThreads = 8;
  constexpr int kIncrementsPerThread = 100;
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      try {
        for (int i = 0; i < kIncrementsPerThread; ++i) client.Execute({"INCR", counter});
      } catch (...) {
        ++failures;
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(client.Execute({"GET", counter}).text, std::to_string(kThreads * kIncrementsPerThread));
}

}  // namespace
}  // namespace taskflow
