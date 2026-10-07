#pragma once

#include <hiredis/hiredis.h>

#include <memory>
#include <string>
#include <vector>

#include "taskflow/common/resource_pool.hpp"
#include "taskflow/config/config.hpp"

namespace taskflow {

struct RedisReply {
  enum class Type { kNil, kString, kStatus, kInteger, kArray };
  Type type = Type::kNil;
  std::string text;            // kString / kStatus
  long long integer = 0;       // kInteger
  std::vector<RedisReply> elements;  // kArray

  bool IsNil() const { return type == Type::kNil; }
};

// One Redis connection (AUTH and SELECT already applied). Not thread-safe;
// RedisClient guarantees exclusive use through pooled leases.
class RedisConnection {
 public:
  // Throws RedisError if the server is unreachable or rejects AUTH/SELECT.
  static std::unique_ptr<RedisConnection> Connect(const RedisConfig& config);

  RedisConnection(const RedisConnection&) = delete;
  RedisConnection& operator=(const RedisConnection&) = delete;
  ~RedisConnection();

  bool IsHealthy() const { return context_->err == 0; }

  // Sends one command. Throws RedisError for transport failures and for
  // error replies (e.g. WRONGTYPE).
  RedisReply Command(const std::vector<std::string>& args);

 private:
  explicit RedisConnection(redisContext* context) : context_(context) {}

  redisContext* context_;
};

// Phase 1 scope: connectivity and generic command execution only. Queues,
// delayed sets and heartbeat keys are built on top of this in later phases.
class RedisClient {
 public:
  explicit RedisClient(const RedisConfig& config);

  // Executes a command on a pooled connection. Connections that hit a
  // transport error are discarded.
  RedisReply Execute(const std::vector<std::string>& args);

  // PING; throws RedisError if Redis is unreachable or answers unexpectedly.
  void Ping();

  const RedisConfig& config() const { return config_; }

 private:
  RedisConfig config_;
  ResourcePool<RedisConnection> pool_;
};

}  // namespace taskflow
