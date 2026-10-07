#include <gtest/gtest.h>

#include <cmath>
#include <limits>

#include "taskflow/common/errors.hpp"
#include "taskflow/jobs/job.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;

NewJob ValidJob() {
  NewJob job;
  job.type = "email.send";
  return job;
}

// ---------- NewJob ----------

TEST(NewJobTest, DefaultsAreSensibleAndValid) {
  const NewJob job = ValidJob();
  EXPECT_NO_THROW(job.Validate());
  EXPECT_EQ(job.priority, JobPriority::kNormal);
  EXPECT_EQ(job.initial_state, JobState::kPending);
  EXPECT_TRUE(job.payload.isObject());
  EXPECT_TRUE(job.metadata.isObject());
  EXPECT_FALSE(job.scheduled_at.has_value());
  EXPECT_FALSE(job.idempotency_key.has_value());
  EXPECT_EQ(job.retry.max_attempts, 3);
  EXPECT_EQ(job.timeout, 300000ms);
}

TEST(NewJobTest, TypeMustBeAShortIdentifier) {
  NewJob job = ValidJob();
  for (const char* good : {"a", "email.send", "billing/invoice:v2", "Report_Daily-1"}) {
    job.type = good;
    EXPECT_NO_THROW(job.Validate()) << good;
  }
  for (const char* bad : {"", "has space", "semi;colon", "quote'", "ünicode", "new\nline"}) {
    job.type = bad;
    EXPECT_THROW(job.Validate(), ValidationError) << bad;
  }
  job.type = std::string(kMaxTypeLength, 'a');
  EXPECT_NO_THROW(job.Validate());
  job.type = std::string(kMaxTypeLength + 1, 'a');
  EXPECT_THROW(job.Validate(), ValidationError);
}

TEST(NewJobTest, PayloadAndMetadataMustBeJsonObjectsOfBoundedSize) {
  NewJob job = ValidJob();
  job.payload = Json::Value(Json::arrayValue);
  EXPECT_THROW(job.Validate(), ValidationError);
  job.payload = Json::Value("a string");
  EXPECT_THROW(job.Validate(), ValidationError);

  job = ValidJob();
  job.metadata = Json::Value(42);
  EXPECT_THROW(job.Validate(), ValidationError);

  job = ValidJob();
  job.payload["blob"] = std::string(kMaxPayloadBytes, 'x');
  EXPECT_THROW(job.Validate(), ValidationError);

  job = ValidJob();
  job.payload["nested"]["list"][0] = 1;
  job.payload["text"] = "héllo";
  EXPECT_NO_THROW(job.Validate());
}

TEST(NewJobTest, TimeoutMustBePositiveAndAtMostSevenDays) {
  NewJob job = ValidJob();
  job.timeout = 0ms;
  EXPECT_THROW(job.Validate(), ValidationError);
  job.timeout = -5ms;
  EXPECT_THROW(job.Validate(), ValidationError);
  job.timeout = 1ms;
  EXPECT_NO_THROW(job.Validate());
  job.timeout = kMaxTimeout;
  EXPECT_NO_THROW(job.Validate());
  job.timeout = kMaxTimeout + 1ms;
  EXPECT_THROW(job.Validate(), ValidationError);
}

TEST(NewJobTest, OnlyPendingAndReadyAreAcceptedAsInitialStates) {
  NewJob job = ValidJob();
  for (const auto state : {JobState::kRunning, JobState::kCompleted, JobState::kFailed,
                           JobState::kRetryWait, JobState::kTimeout, JobState::kCancelled}) {
    job.initial_state = state;
    EXPECT_THROW(job.Validate(), ValidationError) << ToString(state);
  }
  job.initial_state = JobState::kReady;
  EXPECT_NO_THROW(job.Validate());
}

TEST(NewJobTest, AReadyJobCannotBeScheduledInTheFuture) {
  NewJob job = ValidJob();
  job.initial_state = JobState::kReady;
  job.scheduled_at = std::chrono::system_clock::now() + 1h;
  EXPECT_THROW(job.Validate(), ValidationError);
  job.initial_state = JobState::kPending;  // a delayed job is PENDING
  EXPECT_NO_THROW(job.Validate());
}

TEST(NewJobTest, IdempotencyKeyRules) {
  NewJob job = ValidJob();
  job.idempotency_key = "order-1234";
  EXPECT_NO_THROW(job.Validate());
  job.idempotency_key = "";
  EXPECT_THROW(job.Validate(), ValidationError);
  job.idempotency_key = std::string(kMaxIdempotencyKeyLength + 1, 'k');
  EXPECT_THROW(job.Validate(), ValidationError);
  job.idempotency_key = std::string("bad\x01key");
  EXPECT_THROW(job.Validate(), ValidationError);
  job.idempotency_key = std::string(kMaxIdempotencyKeyLength, 'k');
  EXPECT_NO_THROW(job.Validate());
}

TEST(NewJobTest, InvalidRetryPolicyIsRejectedThroughTheJob) {
  NewJob job = ValidJob();
  job.retry.max_attempts = 0;
  EXPECT_THROW(job.Validate(), ValidationError);
}

// ---------- RetryPolicy ----------

TEST(RetryPolicyTest, DefaultsAreValid) {
  EXPECT_NO_THROW(RetryPolicy{}.Validate());
}

TEST(RetryPolicyTest, MaxAttemptsBounds) {
  RetryPolicy policy;
  policy.max_attempts = 1;  // "no retries"
  EXPECT_NO_THROW(policy.Validate());
  policy.max_attempts = kMaxAttemptsLimit;
  EXPECT_NO_THROW(policy.Validate());
  policy.max_attempts = 0;
  EXPECT_THROW(policy.Validate(), ValidationError);
  policy.max_attempts = -1;
  EXPECT_THROW(policy.Validate(), ValidationError);
  policy.max_attempts = kMaxAttemptsLimit + 1;
  EXPECT_THROW(policy.Validate(), ValidationError);
}

TEST(RetryPolicyTest, DelayBounds) {
  RetryPolicy policy;
  policy.initial_delay = -1ms;
  EXPECT_THROW(policy.Validate(), ValidationError);

  policy = {};
  policy.initial_delay = 0ms;
  policy.max_delay = 0ms;
  EXPECT_NO_THROW(policy.Validate());

  policy = {};
  policy.initial_delay = 10s;
  policy.max_delay = 5s;  // below the initial delay
  EXPECT_THROW(policy.Validate(), ValidationError);

  policy = {};
  policy.max_delay = std::chrono::hours(24 * 8);
  EXPECT_THROW(policy.Validate(), ValidationError);
}

TEST(RetryPolicyTest, BackoffMultiplierBounds) {
  RetryPolicy policy;
  policy.backoff_multiplier = 1.0;  // constant delay
  EXPECT_NO_THROW(policy.Validate());
  policy.backoff_multiplier = 100.0;
  EXPECT_NO_THROW(policy.Validate());
  for (const double bad : {0.99, 0.0, -2.0, 100.01, std::numeric_limits<double>::quiet_NaN(),
                           std::numeric_limits<double>::infinity()}) {
    policy.backoff_multiplier = bad;
    EXPECT_THROW(policy.Validate(), ValidationError) << bad;
  }
}

// ---------- priority / id ----------

TEST(JobPriorityTest, OrderingMatchesUrgency) {
  EXPECT_LT(ToInt(JobPriority::kLow), ToInt(JobPriority::kNormal));
  EXPECT_LT(ToInt(JobPriority::kNormal), ToInt(JobPriority::kHigh));
  EXPECT_LT(ToInt(JobPriority::kHigh), ToInt(JobPriority::kCritical));
}

TEST(JobPriorityTest, NamesAndNumbersRoundTrip) {
  for (const auto priority : {JobPriority::kLow, JobPriority::kNormal, JobPriority::kHigh,
                              JobPriority::kCritical}) {
    EXPECT_EQ(ParseJobPriority(ToString(priority)), priority);
    EXPECT_EQ(JobPriorityFromInt(ToInt(priority)), priority);
  }
  EXPECT_THROW(ParseJobPriority("urgent"), ValidationError);
  EXPECT_THROW(JobPriorityFromInt(4), ValidationError);
  EXPECT_THROW(JobPriorityFromInt(-1), ValidationError);
}

TEST(JobIdTest, ParsesUuidsAndNormalizesCase) {
  const JobId id = JobId::Parse("123E4567-E89B-12D3-A456-426614174000");
  EXPECT_EQ(id.str(), "123e4567-e89b-12d3-a456-426614174000");
  EXPECT_EQ(id, JobId::Parse("123e4567-e89b-12d3-a456-426614174000"));
  EXPECT_FALSE(id.empty());
  EXPECT_TRUE(JobId{}.empty());
}

TEST(JobIdTest, RejectsAnythingThatIsNotAUuid) {
  for (const char* bad : {"", "not-a-uuid", "123e4567e89b12d3a456426614174000",
                          "123e4567-e89b-12d3-a456-42661417400",   // too short
                          "123e4567-e89b-12d3-a456-4266141740000",  // too long
                          "123e4567-e89b-12d3-a456-42661417400g",   // non-hex
                          "'; DROP TABLE jobs; --"}) {
    EXPECT_THROW(JobId::Parse(bad), ValidationError) << bad;
  }
}

// ---------- JobUpdate ----------

TEST(JobUpdateTest, AnEmptyUpdateIsRejected) {
  EXPECT_THROW(JobUpdate{}.Validate(), ValidationError);
}

TEST(JobUpdateTest, EachEditableFieldIsValidated) {
  JobUpdate update;
  update.priority = JobPriority::kHigh;
  EXPECT_NO_THROW(update.Validate());

  update = {};
  update.payload = Json::Value(Json::arrayValue);
  EXPECT_THROW(update.Validate(), ValidationError);

  update = {};
  update.timeout = 0ms;
  EXPECT_THROW(update.Validate(), ValidationError);

  update = {};
  update.retry = RetryPolicy{};
  update.retry->max_attempts = 0;
  EXPECT_THROW(update.Validate(), ValidationError);

  update = {};
  update.expected_version = 3;  // a version alone changes nothing
  EXPECT_THROW(update.Validate(), ValidationError);
}

// ---------- attempts and outcomes ----------

TEST(AttemptStateTest, NamesRoundTrip) {
  for (const auto state : {AttemptState::kRunning, AttemptState::kSucceeded, AttemptState::kFailed,
                           AttemptState::kTimedOut, AttemptState::kCancelled}) {
    EXPECT_EQ(ParseAttemptState(ToString(state)), state);
  }
  EXPECT_EQ(ToString(AttemptState::kTimedOut), "TIMED_OUT");
  EXPECT_THROW(ParseAttemptState("DONE"), ValidationError);
}

TEST(AttemptOutcomeTest, SuccessMayCarryOutputButNoError) {
  AttemptOutcome outcome;
  outcome.state = AttemptState::kSucceeded;
  EXPECT_NO_THROW(outcome.Validate());
  outcome.output = Json::Value(Json::objectValue);
  EXPECT_NO_THROW(outcome.Validate());
  outcome.error_message = "oops";
  EXPECT_THROW(outcome.Validate(), ValidationError);
}

TEST(AttemptOutcomeTest, FailuresRequireAnErrorMessageAndForbidOutput) {
  for (const auto state :
       {AttemptState::kFailed, AttemptState::kTimedOut, AttemptState::kCancelled}) {
    AttemptOutcome outcome;
    outcome.state = state;
    EXPECT_THROW(outcome.Validate(), ValidationError) << ToString(state);  // no message
    outcome.error_message = "";
    EXPECT_THROW(outcome.Validate(), ValidationError) << ToString(state);  // empty message
    outcome.error_message = "boom";
    EXPECT_NO_THROW(outcome.Validate()) << ToString(state);
    outcome.output = Json::Value(Json::objectValue);
    EXPECT_THROW(outcome.Validate(), ValidationError) << ToString(state);  // output on failure
  }
}

TEST(AttemptOutcomeTest, RunningIsNotAnOutcome) {
  AttemptOutcome outcome;
  outcome.state = AttemptState::kRunning;
  EXPECT_THROW(outcome.Validate(), ValidationError);
}

}  // namespace
}  // namespace taskflow
