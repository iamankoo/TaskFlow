#include <gtest/gtest.h>

#include <set>
#include <utility>

#include "taskflow/common/errors.hpp"
#include "taskflow/jobs/job_state.hpp"

namespace taskflow {
namespace {

using S = JobState;

// The complete, independently written expectation of the state machine. If the
// production table drifts, this test (and the database parity test) fails.
const std::set<std::pair<JobState, JobState>>& ExpectedTransitions() {
  static const std::set<std::pair<JobState, JobState>> expected = {
      {S::kPending, S::kReady},      {S::kPending, S::kCancelled},
      {S::kReady, S::kRunning},      {S::kReady, S::kCancelled},
      {S::kRunning, S::kCompleted},  {S::kRunning, S::kFailed},
      {S::kRunning, S::kRetryWait},  {S::kRunning, S::kTimeout},
      {S::kRunning, S::kCancelled},  {S::kRetryWait, S::kReady},
      {S::kRetryWait, S::kCancelled}, {S::kFailed, S::kReady},
      {S::kTimeout, S::kReady},
  };
  return expected;
}

TEST(JobStateTest, ThereAreExactlyEightStates) {
  EXPECT_EQ(kAllJobStates.size(), 8u);
  std::set<std::string_view> names;
  for (const auto state : kAllJobStates) names.insert(ToString(state));
  EXPECT_EQ(names.size(), 8u);
}

TEST(JobStateTest, NamesRoundTripThroughParse) {
  for (const auto state : kAllJobStates) EXPECT_EQ(ParseJobState(ToString(state)), state);
  EXPECT_EQ(ToString(S::kRetryWait), "RETRY_WAIT");
}

TEST(JobStateTest, ParseRejectsUnknownAndWronglyCasedNames) {
  EXPECT_THROW(ParseJobState("DONE"), ValidationError);
  EXPECT_THROW(ParseJobState("ready"), ValidationError);
  EXPECT_THROW(ParseJobState(""), ValidationError);
}

TEST(JobStateTest, EveryPairOfStatesMatchesTheDocumentedTable) {
  int allowed = 0;
  for (const auto from : kAllJobStates) {
    for (const auto to : kAllJobStates) {
      const bool expected = ExpectedTransitions().count({from, to}) == 1;
      EXPECT_EQ(CanTransition(from, to), expected)
          << ToString(from) << " -> " << ToString(to);
      allowed += CanTransition(from, to) ? 1 : 0;
    }
  }
  EXPECT_EQ(allowed, static_cast<int>(ExpectedTransitions().size()));
}

TEST(JobStateTest, NoStateTransitionsToItself) {
  for (const auto state : kAllJobStates) EXPECT_FALSE(CanTransition(state, state)) << ToString(state);
}

TEST(JobStateTest, AllowedTransitionsListAgreesWithCanTransition) {
  for (const auto from : kAllJobStates) {
    for (const auto to : AllowedTransitions(from)) EXPECT_TRUE(CanTransition(from, to));
  }
}

TEST(JobStateTest, FinishedJobsCannotRunAgain) {
  for (const auto from : {S::kCompleted, S::kCancelled}) {
    EXPECT_TRUE(AllowedTransitions(from).empty()) << ToString(from);
    EXPECT_FALSE(CanTransition(from, S::kRunning));
    EXPECT_FALSE(CanTransition(from, S::kReady));
  }
  EXPECT_FALSE(CanTransition(S::kFailed, S::kRunning));
  EXPECT_FALSE(CanTransition(S::kTimeout, S::kRunning));
}

TEST(JobStateTest, ManualRequeueIsTheOnlyWayOutOfFailedAndTimeout) {
  EXPECT_EQ(AllowedTransitions(S::kFailed).size(), 1u);
  EXPECT_TRUE(CanTransition(S::kFailed, S::kReady));
  EXPECT_EQ(AllowedTransitions(S::kTimeout).size(), 1u);
  EXPECT_TRUE(CanTransition(S::kTimeout, S::kReady));
}

TEST(JobStateTest, ARunningJobCanOnlyFinishOrBeCancelled) {
  EXPECT_FALSE(CanTransition(S::kRunning, S::kReady));
  EXPECT_FALSE(CanTransition(S::kRunning, S::kPending));
  EXPECT_FALSE(CanTransition(S::kPending, S::kRunning));  // must be claimed from READY
  EXPECT_FALSE(CanTransition(S::kRetryWait, S::kRunning));  // must pass through READY
}

TEST(JobStateTest, TerminalStatesAreTheFourFinalOnes) {
  for (const auto state : kAllJobStates) {
    const bool expected = state == S::kCompleted || state == S::kFailed || state == S::kTimeout ||
                          state == S::kCancelled;
    EXPECT_EQ(IsTerminal(state), expected) << ToString(state);
  }
}

TEST(JobStateTest, OnlyPendingAndReadyAreValidInitialStates) {
  for (const auto state : kAllJobStates) {
    EXPECT_EQ(IsValidInitialState(state), state == S::kPending || state == S::kReady)
        << ToString(state);
  }
}

TEST(JobStateTest, ValidateTransitionAcceptsLegalAndNamesBothStatesWhenRejecting) {
  EXPECT_NO_THROW(ValidateTransition(S::kReady, S::kRunning));
  try {
    ValidateTransition(S::kCompleted, S::kRunning);
    FAIL() << "expected InvalidTransitionError";
  } catch (const InvalidTransitionError& e) {
    const std::string message = e.what();
    EXPECT_NE(message.find("COMPLETED"), std::string::npos) << message;
    EXPECT_NE(message.find("RUNNING"), std::string::npos) << message;
    EXPECT_EQ(e.code(), ErrorCode::kInvalidTransition);
  }
  EXPECT_THROW(ValidateTransition(S::kCancelled, S::kRunning), InvalidTransitionError);
}

}  // namespace
}  // namespace taskflow
