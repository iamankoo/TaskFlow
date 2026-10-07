#include "taskflow/redis/redis_client.hpp"

#include <chrono>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

struct ReplyDeleter {
  void operator()(redisReply* reply) const { freeReplyObject(reply); }
};
using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;

timeval ToTimeval(unsigned milliseconds) {
  timeval tv{};
  tv.tv_sec = static_cast<time_t>(milliseconds / 1000);
  tv.tv_usec = static_cast<suseconds_t>((milliseconds % 1000) * 1000);
  return tv;
}

RedisReply Convert(const redisReply& reply) {
  RedisReply out;
  switch (reply.type) {
    case REDIS_REPLY_STRING:
      out.type = RedisReply::Type::kString;
      out.text.assign(reply.str, reply.len);
      break;
    case REDIS_REPLY_STATUS:
      out.type = RedisReply::Type::kStatus;
      out.text.assign(reply.str, reply.len);
      break;
    case REDIS_REPLY_INTEGER:
      out.type = RedisReply::Type::kInteger;
      out.integer = reply.integer;
      break;
    case REDIS_REPLY_ARRAY:
      out.type = RedisReply::Type::kArray;
      for (std::size_t i = 0; i < reply.elements; ++i) {
        out.elements.push_back(Convert(*reply.element[i]));
      }
      break;
    default:
      out.type = RedisReply::Type::kNil;
  }
  return out;
}

}  // namespace

std::unique_ptr<RedisConnection> RedisConnection::Connect(const RedisConfig& config) {
  const timeval connect_timeout = ToTimeval(config.connect_timeout_ms);
  redisContext* raw = redisConnectWithTimeout(config.host.c_str(), config.port, connect_timeout);
  if (raw == nullptr) throw RedisError("Redis connection allocation failed");
  if (raw->err != 0) {
    const std::string reason = raw->errstr;
    redisFree(raw);
    throw RedisError("cannot connect to Redis at " + config.host + ":" +
                     std::to_string(config.port) + ": " + reason);
  }
  std::unique_ptr<RedisConnection> connection(new RedisConnection(raw));
  redisSetTimeout(raw, ToTimeval(config.command_timeout_ms));

  if (!config.password.empty()) connection->Command({"AUTH", config.password});
  if (config.database != 0) connection->Command({"SELECT", std::to_string(config.database)});
  return connection;
}

RedisConnection::~RedisConnection() { redisFree(context_); }

RedisReply RedisConnection::Command(const std::vector<std::string>& args) {
  if (args.empty()) throw RedisError("empty Redis command");

  std::vector<const char*> argv;
  std::vector<std::size_t> lengths;
  argv.reserve(args.size());
  lengths.reserve(args.size());
  for (const auto& arg : args) {
    argv.push_back(arg.data());
    lengths.push_back(arg.size());
  }

  ReplyPtr reply(static_cast<redisReply*>(
      redisCommandArgv(context_, static_cast<int>(args.size()), argv.data(), lengths.data())));
  if (!reply) {
    throw RedisError(std::string("Redis command '") + args[0] + "' failed: " + context_->errstr);
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    throw RedisError(std::string("Redis command '") + args[0] + "' returned error: " +
                     std::string(reply->str, reply->len));
  }
  return Convert(*reply);
}

RedisClient::RedisClient(const RedisConfig& config)
    : config_(config),
      pool_(
          config.pool_size, [cfg = config] { return RedisConnection::Connect(cfg); },
          [](RedisConnection& connection) { return connection.IsHealthy(); },
          std::chrono::milliseconds(config.acquire_timeout_ms)) {}

RedisReply RedisClient::Execute(const std::vector<std::string>& args) {
  auto lease = pool_.Acquire();
  try {
    return lease->Command(args);
  } catch (...) {
    // A transport failure leaves the socket unusable; an error reply does not.
    if (!lease->IsHealthy()) lease.MarkBroken();
    throw;
  }
}

void RedisClient::Ping() {
  const auto reply = Execute({"PING"});
  if (reply.type != RedisReply::Type::kStatus || reply.text != "PONG") {
    throw RedisError("Redis PING returned an unexpected reply");
  }
}

}  // namespace taskflow
