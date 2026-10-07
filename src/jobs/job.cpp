#include "taskflow/jobs/job.hpp"

#include <cmath>
#include <string>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

constexpr std::size_t kMaxMetadataBytes = 64 * 1024;
constexpr std::size_t kMaxErrorMessageBytes = 64 * 1024;
constexpr std::chrono::milliseconds kMaxRetryDelay = std::chrono::hours(24 * 7);
constexpr double kMaxBackoffMultiplier = 100.0;

std::size_t SerializedSize(const Json::Value& value) {
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  return Json::writeString(builder, value).size();
}

void RequireObject(const Json::Value& value, const char* name, std::size_t max_bytes) {
  if (!value.isObject()) throw ValidationError(std::string(name) + " must be a JSON object");
  if (SerializedSize(value) > max_bytes) {
    throw ValidationError(std::string(name) + " exceeds " + std::to_string(max_bytes) + " bytes");
  }
}

void ValidateTimeout(std::chrono::milliseconds timeout) {
  if (timeout.count() < 1 || timeout > kMaxTimeout) {
    throw ValidationError("timeout must be between 1 ms and 7 days");
  }
}

bool IsHex(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

}  // namespace

// ---------- JobPriority ----------

std::string_view ToString(JobPriority priority) noexcept {
  switch (priority) {
    case JobPriority::kLow: return "LOW";
    case JobPriority::kNormal: return "NORMAL";
    case JobPriority::kHigh: return "HIGH";
    case JobPriority::kCritical: return "CRITICAL";
  }
  return "UNKNOWN";
}

JobPriority ParseJobPriority(std::string_view text) {
  for (const auto priority : {JobPriority::kLow, JobPriority::kNormal, JobPriority::kHigh,
                              JobPriority::kCritical}) {
    if (ToString(priority) == text) return priority;
  }
  throw ValidationError("unknown job priority '" + std::string(text) + "'");
}

JobPriority JobPriorityFromInt(int value) {
  if (value < 0 || value > 3) {
    throw ValidationError("job priority out of range: " + std::to_string(value));
  }
  return static_cast<JobPriority>(value);
}

// ---------- JobId ----------

JobId JobId::Parse(std::string_view text) {
  // 8-4-4-4-12 hexadecimal digits.
  if (text.size() != 36) throw ValidationError("job id must be a UUID");
  std::string normalized(text);
  for (std::size_t i = 0; i < normalized.size(); ++i) {
    const bool dash = (i == 8 || i == 13 || i == 18 || i == 23);
    if (dash ? normalized[i] != '-' : !IsHex(normalized[i])) {
      throw ValidationError("job id must be a UUID");
    }
    if (normalized[i] >= 'A' && normalized[i] <= 'F') normalized[i] = static_cast<char>(normalized[i] + 32);
  }
  return JobId(std::move(normalized));
}

// ---------- RetryPolicy / NewJob / JobUpdate ----------

void RetryPolicy::Validate() const {
  if (max_attempts < 1 || max_attempts > kMaxAttemptsLimit) {
    throw ValidationError("retry.max_attempts must be between 1 and " +
                          std::to_string(kMaxAttemptsLimit));
  }
  if (initial_delay.count() < 0 || initial_delay > kMaxRetryDelay) {
    throw ValidationError("retry.initial_delay must be between 0 and 7 days");
  }
  if (!std::isfinite(backoff_multiplier) || backoff_multiplier < 1.0 ||
      backoff_multiplier > kMaxBackoffMultiplier) {
    throw ValidationError("retry.backoff_multiplier must be between 1 and 100");
  }
  if (max_delay < initial_delay || max_delay > kMaxRetryDelay) {
    throw ValidationError("retry.max_delay must be >= initial_delay and at most 7 days");
  }
}

void NewJob::Validate() const {
  if (type.empty() || type.size() > kMaxTypeLength) {
    throw ValidationError("job type must be 1-" + std::to_string(kMaxTypeLength) + " characters");
  }
  for (const char c : type) {
    const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                    c == '.' || c == '_' || c == '-' || c == ':' || c == '/';
    if (!ok) throw ValidationError("job type may only contain letters, digits and . _ - : /");
  }
  RequireObject(payload, "payload", kMaxPayloadBytes);
  RequireObject(metadata, "metadata", kMaxMetadataBytes);
  retry.Validate();
  ValidateTimeout(timeout);
  if (!IsValidInitialState(initial_state)) {
    throw ValidationError("a new job must start PENDING or READY, not " +
                          std::string(ToString(initial_state)));
  }
  if (initial_state == JobState::kReady && scheduled_at &&
      *scheduled_at > std::chrono::system_clock::now()) {
    throw ValidationError("a READY job cannot be scheduled in the future; create it PENDING");
  }
  if (idempotency_key) {
    if (idempotency_key->empty() || idempotency_key->size() > kMaxIdempotencyKeyLength) {
      throw ValidationError("idempotency key must be 1-" +
                            std::to_string(kMaxIdempotencyKeyLength) + " characters");
    }
    for (const char c : *idempotency_key) {
      if (static_cast<unsigned char>(c) < 0x20 || c == 0x7f) {
        throw ValidationError("idempotency key must not contain control characters");
      }
    }
  }
}

void JobUpdate::Validate() const {
  if (payload) RequireObject(*payload, "payload", kMaxPayloadBytes);
  if (metadata) RequireObject(*metadata, "metadata", kMaxMetadataBytes);
  if (retry) retry->Validate();
  if (timeout) ValidateTimeout(*timeout);
  if (!payload && !metadata && !priority && !scheduled_at && !retry && !timeout) {
    throw ValidationError("job update does not change anything");
  }
}

// ---------- attempts and results ----------

std::string_view ToString(AttemptState state) noexcept {
  switch (state) {
    case AttemptState::kRunning: return "RUNNING";
    case AttemptState::kSucceeded: return "SUCCEEDED";
    case AttemptState::kFailed: return "FAILED";
    case AttemptState::kTimedOut: return "TIMED_OUT";
    case AttemptState::kCancelled: return "CANCELLED";
  }
  return "UNKNOWN";
}

AttemptState ParseAttemptState(std::string_view text) {
  for (const auto state : {AttemptState::kRunning, AttemptState::kSucceeded, AttemptState::kFailed,
                           AttemptState::kTimedOut, AttemptState::kCancelled}) {
    if (ToString(state) == text) return state;
  }
  throw ValidationError("unknown attempt state '" + std::string(text) + "'");
}

void AttemptOutcome::Validate() const {
  if (state == AttemptState::kRunning) {
    throw ValidationError("an attempt outcome cannot be RUNNING");
  }
  if (success()) {
    if (error_message) throw ValidationError("a successful attempt cannot carry an error message");
    if (output && SerializedSize(*output) > kMaxPayloadBytes) {
      throw ValidationError("result output exceeds " + std::to_string(kMaxPayloadBytes) + " bytes");
    }
    return;
  }
  if (output) throw ValidationError("only a successful attempt can carry output");
  if (!error_message || error_message->empty()) {
    throw ValidationError("a failed attempt requires an error message");
  }
  if (error_message->size() > kMaxErrorMessageBytes) {
    throw ValidationError("error message exceeds " + std::to_string(kMaxErrorMessageBytes) + " bytes");
  }
}

}  // namespace taskflow
