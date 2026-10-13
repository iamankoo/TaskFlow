#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>

#include "scheduler_fixture.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;
using testing::SchedulerFixture;

class SchedulerRepositoryTest : public SchedulerFixture {};

TEST_F(SchedulerRepositoryTest, FindDueJobsReturnsWaitingJobsAtOrBeforeNow) {
  const auto past = Pending(JobPriority::kNormal, -10s);
  const auto exactly_now = Pending(JobPriority::kNormal, 0s);
  const auto future = Pending(JobPriority::kNormal, 1s);
  const auto ready = Ready(JobPriority::kNormal, -5s);  // already READY: not a candidate

  const auto due = repo_->FindDueJobs(base_, 100);
  std::set<std::int64_t> sequences;
  for (const auto& e : due) sequences.insert(e.sequence);
  EXPECT_TRUE(sequences.count(past.sequence));
  EXPECT_TRUE(sequences.count(exactly_now.sequence)) << "scheduled_at == now is due";
  EXPECT_FALSE(sequences.count(future.sequence));
  EXPECT_FALSE(sequences.count(ready.sequence));
}

TEST_F(SchedulerRepositoryTest, DueBoundaryIsExactToTheMicrosecond) {
  const auto job = Pending(JobPriority::kHigh, 30s);
  EXPECT_TRUE(repo_->FindDueJobs(base_ + 30s - 1us, 10).empty());
  EXPECT_EQ(repo_->FindDueJobs(base_ + 30s, 10).size(), 1u);
  EXPECT_EQ(repo_->FindDueJobs(base_ + 30s + 1us, 10).size(), 1u);
  EXPECT_EQ(repo_->FindDueJobs(base_ + 30s, 10).front().sequence, job.sequence);
}

TEST_F(SchedulerRepositoryTest, FindDueJobsLimitsPerPriorityAndReturnsOldestFirst) {
  for (int i = 0; i < 5; ++i) {
    for (const JobPriority p : kPrioritiesHighestFirst) Pending(p, Ms(-1000 + i));
  }
  const auto due = repo_->FindDueJobs(base_, 2);
  EXPECT_EQ(due.size(), 8u);  // 2 per priority
  for (const JobPriority p : kPrioritiesHighestFirst) {
    std::vector<SchedulingEntry> of_priority;
    for (const auto& e : due) {
      if (e.priority == p) of_priority.push_back(e);
    }
    ASSERT_EQ(of_priority.size(), 2u);
    EXPECT_LE(of_priority[0].scheduled_at, of_priority[1].scheduled_at);
    EXPECT_LT(of_priority[0].sequence, of_priority[1].sequence);  // oldest two of the five
  }
}

TEST_F(SchedulerRepositoryTest, RetryWaitJobsAreDueToo) {
  const auto job = Ready(JobPriority::kNormal, -1s);
  repo_->CreateAttempt(job.id);
  AttemptOutcome failure;
  failure.state = AttemptState::kFailed;
  failure.error_message = "boom";
  StateChange backoff;
  backoff.scheduled_at = base_ + 5s;
  repo_->StoreResult(job.id, 1, failure, JobState::kRetryWait, backoff);

  EXPECT_TRUE(repo_->FindDueJobs(base_ + 4s, 10).empty());
  const auto due = repo_->FindDueJobs(base_ + 5s, 10);
  ASSERT_EQ(due.size(), 1u);
  EXPECT_EQ(due[0].state, JobState::kRetryWait);
}

TEST_F(SchedulerRepositoryTest, PromoteMovesDueWaitingJobsToReadyAndAuditsThem) {
  const auto a = Pending(JobPriority::kLow, -5s);
  const auto b = Pending(JobPriority::kCritical, -1s);
  const auto promoted = repo_->PromoteJobsToReady({b.id, a.id}, base_);

  ASSERT_EQ(promoted.size(), 2u);
  EXPECT_EQ(promoted[0].id, b.id);  // order of the request is preserved
  EXPECT_EQ(promoted[1].id, a.id);
  EXPECT_EQ(promoted[0].state, JobState::kReady);
  EXPECT_EQ(StateOf(a), JobState::kReady);
  EXPECT_EQ(repo_->GetJob(a.id)->version, 2);

  const auto events = repo_->GetEvents(a.id);
  ASSERT_EQ(events.size(), 2u);
  EXPECT_EQ(events[1].from_state, JobState::kPending);
  EXPECT_EQ(events[1].to_state, JobState::kReady);
  EXPECT_EQ(events[1].reason, "scheduled time reached");
}

TEST_F(SchedulerRepositoryTest, PromoteSkipsJobsThatAreNotDueOrNotWaiting) {
  const auto future = Pending(JobPriority::kHigh, 10s);
  const auto already_ready = Ready(JobPriority::kHigh, -1s);
  const auto due = Pending(JobPriority::kHigh, -1s);
  const JobId unknown = JobId::Parse("99999999-9999-9999-9999-999999999999");

  const auto promoted =
      repo_->PromoteJobsToReady({future.id, already_ready.id, unknown, due.id}, base_);
  ASSERT_EQ(promoted.size(), 1u);
  EXPECT_EQ(promoted[0].id, due.id);
  EXPECT_EQ(StateOf(future), JobState::kPending);
  EXPECT_EQ(repo_->GetEvents(already_ready.id).size(), 1u);  // untouched
  EXPECT_TRUE(repo_->PromoteJobsToReady({}, base_).empty());
}

TEST_F(SchedulerRepositoryTest, PromotingTwiceIsANoOp) {
  const auto job = Pending(JobPriority::kNormal, -1s);
  EXPECT_EQ(repo_->PromoteJobsToReady({job.id}, base_).size(), 1u);
  EXPECT_TRUE(repo_->PromoteJobsToReady({job.id}, base_).empty());
  EXPECT_EQ(repo_->GetEvents(job.id).size(), 2u);
}

TEST_F(SchedulerRepositoryTest, ConcurrentPromotersNeverPromoteAJobTwice) {
  std::vector<JobId> ids;
  for (int i = 0; i < 60; ++i) ids.push_back(Pending(JobPriority::kNormal, -1s).id);

  std::atomic<int> total{0};
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      try {
        total += static_cast<int>(repo_->PromoteJobsToReady(ids, base_).size());
      } catch (...) {
        ++failures;
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(total.load(), 60);                                              // each job exactly once
  EXPECT_EQ(Count("job_events", "to_status = 'READY' AND reason = 'scheduled time reached'"), 60);
  EXPECT_EQ(Count("jobs", "status = 'READY' AND version = 2"), 60);          // no double update
}

TEST_F(SchedulerRepositoryTest, ReadyScanPagesInOrderWithoutGapsOrRepeats) {
  std::vector<std::int64_t> created;
  for (int i = 0; i < 25; ++i) created.push_back(Ready(JobPriority::kHigh, Ms(-5000 + i / 5)).sequence);
  Ready(JobPriority::kLow, -1s);  // a different priority must not appear

  std::vector<std::int64_t> scanned;
  std::optional<ScanCursor> cursor;
  int pages = 0;
  for (;;) {
    const auto page = repo_->FindReadyJobs(JobPriority::kHigh, cursor, 10);
    ++pages;
    for (const auto& e : page) scanned.push_back(e.sequence);
    if (page.size() < 10) break;
    cursor = ScanCursor{page.back().scheduled_at, page.back().sequence};
  }
  EXPECT_EQ(pages, 3);
  EXPECT_EQ(scanned, created);  // creation order == (scheduled_at, sequence) order here
}

TEST_F(SchedulerRepositoryTest, WaitingScanCoversPendingAndRetryWaitOnly) {
  const auto pending_a = Pending(JobPriority::kLow, 5s);
  const auto pending_b = Pending(JobPriority::kHigh, -5s);
  Ready(JobPriority::kNormal, -1s);
  const auto cancelled = Pending(JobPriority::kNormal, 0s);
  repo_->UpdateJobState(cancelled.id, JobState::kCancelled);

  const auto all = repo_->FindWaitingJobs(std::nullopt, 100);
  ASSERT_EQ(all.size(), 2u);
  EXPECT_EQ(all[0].sequence, pending_b.sequence);  // ordered by scheduled_at
  EXPECT_EQ(all[1].sequence, pending_a.sequence);

  const auto after_first = repo_->FindWaitingJobs(ScanCursor{all[0].scheduled_at, all[0].sequence}, 100);
  ASSERT_EQ(after_first.size(), 1u);
  EXPECT_EQ(after_first[0].sequence, pending_a.sequence);
}

TEST_F(SchedulerRepositoryTest, SchedulingEntriesCarryOnlyWhatTheSchedulerNeeds) {
  const auto job = Pending(JobPriority::kHigh, 7s);
  const auto entries = repo_->GetSchedulingEntries({job.id, JobId::Parse("99999999-9999-9999-9999-999999999999")});
  ASSERT_EQ(entries.size(), 1u);  // unknown ids are omitted
  EXPECT_EQ(entries[0].id, job.id);
  EXPECT_EQ(entries[0].priority, JobPriority::kHigh);
  EXPECT_EQ(entries[0].state, JobState::kPending);
  EXPECT_EQ(entries[0].scheduled_at, base_ + 7s);
  EXPECT_TRUE(repo_->GetSchedulingEntries({}).empty());
}

TEST_F(SchedulerRepositoryTest, SchedulingQueriesUseTheRightIndexes) {
  Sql("INSERT INTO jobs (type, status, priority, scheduled_at) "
      "SELECT 't', 'PENDING', i % 4, now() - (i || ' seconds')::interval FROM generate_series(1, 3000) i");
  Sql("INSERT INTO jobs (type, status, priority) SELECT 't', 'READY', i % 4 FROM generate_series(1, 3000) i");
  db_->Exec("ALTER TABLE jobs DISABLE TRIGGER trg_jobs_initial_state");  // seed finished jobs directly
  Sql("INSERT INTO jobs (type, status, finished_at) SELECT 't', 'CANCELLED', now() FROM generate_series(1, 3000) i");
  db_->Exec("ALTER TABLE jobs ENABLE TRIGGER trg_jobs_initial_state");
  Sql("ANALYZE jobs");
  db_->Exec("SET enable_seqscan = off");

  const auto plan_of = [&](const std::string& sql) {
    std::string text;
    for (const auto& row : Sql("EXPLAIN " + sql).rows) text += *row[0] + "\n";
    return text;
  };
  const std::string due = plan_of(
      "SELECT id FROM jobs WHERE status IN ('PENDING','RETRY_WAIT') AND priority = 2 "
      "AND scheduled_at <= now() ORDER BY scheduled_at, sequence LIMIT 100");
  EXPECT_NE(due.find("idx_jobs_due"), std::string::npos) << due;

  const std::string ready = plan_of(
      "SELECT id FROM jobs WHERE status = 'READY' AND priority = 2 "
      "AND (scheduled_at, sequence) > (now() - interval '1 day', 0) ORDER BY scheduled_at, sequence LIMIT 100");
  EXPECT_NE(ready.find("idx_jobs_ready_queue"), std::string::npos) << ready;
}

}  // namespace
}  // namespace taskflow
