#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "scheduler_fixture.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;
using testing::SchedulerFixture;

class SchedulerTest : public SchedulerFixture {
 protected:
  // Ticks until nothing more is promoted; returns the promoted sequences, tick by tick.
  std::vector<std::vector<std::int64_t>> TickUntilIdle(Scheduler& scheduler, int max_ticks = 50) {
    std::vector<std::vector<std::int64_t>> batches;
    for (int i = 0; i < max_ticks; ++i) {
      const auto result = scheduler.Tick();
      if (result.promoted == 0) break;
      batches.push_back(Sequences(result.promoted_entries));
    }
    return batches;
  }
};

// ---------- promotion and delayed jobs ----------

TEST_F(SchedulerTest, PromotesDueJobsAndLeavesFutureJobsDelayed) {
  const auto overdue = Pending(JobPriority::kNormal, -10s);
  const auto exactly_due = Pending(JobPriority::kNormal, 0s);
  const auto soon = Pending(JobPriority::kNormal, 1s);
  const auto later = Pending(JobPriority::kNormal, 30s);

  auto scheduler = MakeScheduler();
  const auto result = scheduler.get()->Tick();
  EXPECT_EQ(result.candidates, 2u);
  EXPECT_EQ(result.promoted, 2u);
  EXPECT_EQ(StateOf(overdue), JobState::kReady);
  EXPECT_EQ(StateOf(exactly_due), JobState::kReady);
  EXPECT_EQ(StateOf(soon), JobState::kPending);
  EXPECT_EQ(StateOf(later), JobState::kPending);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 2u);
  EXPECT_EQ(queue_->DelayedCount(), 2u);  // the delayed index holds exactly the future jobs
}

TEST_F(SchedulerTest, ADelayedJobIsNotPromotedBeforeItsTimeAndIsPromotedAtIt) {
  const auto job = Pending(JobPriority::kHigh, 30s);
  auto scheduler = MakeScheduler();

  now_ = base_;
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  now_ = base_ + 29s + 999'999us;  // one microsecond early
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  EXPECT_EQ(StateOf(job), JobState::kPending);
  EXPECT_EQ(TotalReady(), 0u);

  now_ = base_ + 30s;  // exactly at the scheduled time
  const auto result = scheduler->Tick();
  EXPECT_EQ(result.promoted, 1u);
  EXPECT_EQ(StateOf(job), JobState::kReady);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  EXPECT_EQ(queue_->DelayedCount(), 0u);  // moved out of the delayed index
}

TEST_F(SchedulerTest, ManyDelayedJobsBecomeEligibleInTimeOrderAsTheClockAdvances) {
  std::vector<SchedulingEntry> jobs;
  for (int i = 1; i <= 5; ++i) jobs.push_back(Pending(JobPriority::kNormal, std::chrono::seconds(i * 10)));
  auto scheduler = MakeScheduler();

  for (int step = 0; step <= 5; ++step) {
    now_ = base_ + std::chrono::seconds(step * 10);
    scheduler->Tick();
    for (int i = 0; i < 5; ++i) {
      const bool due = (i + 1) <= step;
      EXPECT_EQ(StateOf(jobs[static_cast<std::size_t>(i)]), due ? JobState::kReady : JobState::kPending)
          << "step " << step << " job " << i;
    }
  }
}

TEST_F(SchedulerTest, DueJobsOfDifferentPrioritiesAreServedByPriority) {
  const auto low = Pending(JobPriority::kLow, -1s);
  const auto high = Pending(JobPriority::kHigh, -1s);
  const auto critical = Pending(JobPriority::kCritical, -1s);
  const auto normal = Pending(JobPriority::kNormal, -1s);
  auto scheduler = MakeScheduler();

  scheduler_config_.max_promotions_per_tick = 1;
  auto one_at_a_time = MakeScheduler("one-at-a-time");
  std::vector<std::int64_t> order;
  for (int i = 0; i < 4; ++i) {
    const auto result = one_at_a_time->Tick();
    ASSERT_EQ(result.promoted_entries.size(), 1u);
    order.push_back(result.promoted_entries[0].sequence);
  }
  EXPECT_EQ(order, (std::vector<std::int64_t>{critical.sequence, high.sequence, normal.sequence, low.sequence}));
}

TEST_F(SchedulerTest, WithinAPriorityPromotionIsFifoBySequence) {
  std::vector<std::int64_t> created;
  for (int i = 0; i < 10; ++i) created.push_back(Pending(JobPriority::kHigh, -1s).sequence);
  scheduler_config_.max_promotions_per_tick = 4;
  auto scheduler = MakeScheduler();

  std::vector<std::int64_t> promoted;
  for (const auto& batch : TickUntilIdle(*scheduler)) promoted.insert(promoted.end(), batch.begin(), batch.end());
  EXPECT_EQ(promoted, created);  // #10 -> #11 -> #12 ...
}

TEST_F(SchedulerTest, FullDrainOrderAcrossTicksIsPriorityThenFifo) {
  const auto l1 = Pending(JobPriority::kLow, -1s);
  const auto l2 = Pending(JobPriority::kLow, -1s);
  const auto n1 = Pending(JobPriority::kNormal, -1s);
  const auto h1 = Pending(JobPriority::kHigh, -1s);
  const auto c1 = Pending(JobPriority::kCritical, -1s);
  const auto c2 = Pending(JobPriority::kCritical, -1s);
  const auto n2 = Pending(JobPriority::kNormal, -1s);
  const auto h2 = Pending(JobPriority::kHigh, -1s);
  scheduler_config_.max_promotions_per_tick = 3;
  auto scheduler = MakeScheduler();

  const auto batches = TickUntilIdle(*scheduler);
  ASSERT_EQ(batches.size(), 3u);
  EXPECT_EQ(batches[0], (std::vector<std::int64_t>{c1.sequence, c2.sequence, h1.sequence}));
  EXPECT_EQ(batches[1], (std::vector<std::int64_t>{h2.sequence, n1.sequence, n2.sequence}));
  EXPECT_EQ(batches[2], (std::vector<std::int64_t>{l1.sequence, l2.sequence}));

  // Workers pulling from Redis see the same order for what is queued.
  std::vector<std::int64_t> popped;
  while (const auto next = queue_->PopNext(0us)) popped.push_back(next->sequence);
  EXPECT_EQ(popped, (std::vector<std::int64_t>{c1.sequence, c2.sequence, h1.sequence, h2.sequence,
                                               n1.sequence, n2.sequence, l1.sequence, l2.sequence}));
}

TEST_F(SchedulerTest, RetryWaitJobsAreAlsoPromotedWhenDue) {
  const auto job = Ready(JobPriority::kNormal, -5s);
  repo_->CreateAttempt(job.id);
  AttemptOutcome failure;
  failure.state = AttemptState::kFailed;
  failure.error_message = "boom";
  StateChange backoff;
  backoff.scheduled_at = base_ + 10s;
  repo_->StoreResult(job.id, 1, failure, JobState::kRetryWait, backoff);

  auto scheduler = MakeScheduler();
  now_ = base_ + 9s;
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  now_ = base_ + 10s;
  EXPECT_EQ(scheduler->Tick().promoted, 1u);
  const auto events = repo_->GetEvents(job.id);
  EXPECT_EQ(events.back().from_state, JobState::kRetryWait);
  EXPECT_EQ(events.back().to_state, JobState::kReady);
}

TEST_F(SchedulerTest, SecondTickPromotesNothingNew) {
  Pending(JobPriority::kNormal, -1s);
  auto scheduler = MakeScheduler();
  EXPECT_EQ(scheduler->Tick().promoted, 1u);
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  EXPECT_EQ(Count("job_events", "to_status = 'READY'"), 1);
}

TEST_F(SchedulerTest, TheSchedulerTouchesOnlyPendingAndRetryWaitJobs) {
  const auto ready = Ready(JobPriority::kNormal, -1s);
  const auto running = Ready(JobPriority::kNormal, -1s);
  repo_->CreateAttempt(running.id);
  const auto cancelled = Pending(JobPriority::kNormal, -1s);
  repo_->UpdateJobState(cancelled.id, JobState::kCancelled);
  const auto completed = Ready(JobPriority::kNormal, -1s);
  repo_->CreateAttempt(completed.id);
  AttemptOutcome ok;
  repo_->StoreResult(completed.id, 1, ok, JobState::kCompleted);

  const auto versions = [&] {
    std::vector<std::int64_t> v;
    for (const auto& e : {ready, running, cancelled, completed}) v.push_back(repo_->GetJob(e.id)->version);
    return v;
  };
  const auto before = versions();
  auto scheduler = MakeScheduler();
  const auto result = scheduler->Tick();
  EXPECT_EQ(result.promoted, 0u);
  EXPECT_EQ(versions(), before);  // not a single row was modified
  EXPECT_EQ(StateOf(running), JobState::kRunning);
  EXPECT_EQ(StateOf(completed), JobState::kCompleted);
  EXPECT_EQ(StateOf(cancelled), JobState::kCancelled);
}

// ---------- aging ----------

TEST_F(SchedulerTest, AgingPromotesAWaitingLowJobAheadOfAFreshCriticalOne) {
  const auto old_low = Pending(JobPriority::kLow, -300s);  // waiting 5 minutes
  const auto fresh_critical = Pending(JobPriority::kCritical, 0s);
  scheduler_config_.max_promotions_per_tick = 1;

  scheduler_config_.aging_interval_ms = 0;  // strict priority: CRITICAL wins
  auto strict = MakeScheduler("strict");
  EXPECT_EQ(strict->Tick().promoted_entries.at(0).sequence, fresh_critical.sequence);
  repo_->UpdateJobState(fresh_critical.id, JobState::kCancelled);  // clear and compare again below

  const auto old_low2 = Pending(JobPriority::kLow, -300s);
  const auto fresh_critical2 = Pending(JobPriority::kCritical, 0s);
  scheduler_config_.aging_interval_ms = 60'000;  // one level per minute
  auto aging = MakeScheduler("aging");
  // old_low (300 s) and old_low2 (300 s) have each waited 5 intervals; both outrank a fresh CRITICAL
  const auto promoted = aging->Tick().promoted_entries.at(0);
  EXPECT_TRUE(promoted.sequence == old_low.sequence || promoted.sequence == old_low2.sequence);
  EXPECT_NE(promoted.sequence, fresh_critical2.sequence);
  EXPECT_EQ(promoted.priority, JobPriority::kLow);
}

TEST_F(SchedulerTest, AgingNeverRewritesThePersistedPriority) {
  const auto low = Pending(JobPriority::kLow, -3600s);
  Pending(JobPriority::kCritical, 0s);
  scheduler_config_.aging_interval_ms = 1000;
  auto scheduler = MakeScheduler();
  TickUntilIdle(*scheduler);

  const Job stored = *repo_->GetJob(low.id);
  EXPECT_EQ(stored.priority, JobPriority::kLow);
  EXPECT_EQ(Count("jobs", "priority = 0"), 1);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kLow), 1u);  // it is queued under its real priority
}

TEST_F(SchedulerTest, HighPriorityKeepsItsAdvantageWhenWaitsAreShort) {
  const auto high = Pending(JobPriority::kHigh, -2s);
  const auto normal = Pending(JobPriority::kNormal, -10s);  // waited 8 s longer, far below one interval
  scheduler_config_.aging_interval_ms = 60'000;
  scheduler_config_.max_promotions_per_tick = 1;
  auto scheduler = MakeScheduler();
  EXPECT_EQ(scheduler->Tick().promoted_entries.at(0).sequence, high.sequence);
  (void)normal;
}

TEST_F(SchedulerTest, LowJobIsNotStarvedByContinuousCriticalTraffic) {
  // Each simulated second one CRITICAL job becomes due and the scheduler can promote one job.
  const auto low = Pending(JobPriority::kLow, 0s);
  scheduler_config_.max_promotions_per_tick = 1;
  scheduler_config_.aging_interval_ms = 10'000;  // LOW overtakes after 3 intervals = 30 s
  auto scheduler = MakeScheduler();

  int promoted_low_at = -1;
  for (int second = 1; second <= 120 && promoted_low_at < 0; ++second) {
    Pending(JobPriority::kCritical, std::chrono::seconds(second));
    now_ = base_ + std::chrono::seconds(second);
    const auto result = scheduler->Tick();
    if (!result.promoted_entries.empty() && result.promoted_entries[0].sequence == low.sequence) {
      promoted_low_at = second;
    }
  }
  ASSERT_GT(promoted_low_at, 0) << "the LOW job starved despite aging";
  EXPECT_LE(promoted_low_at, 31);
}

// ---------- Redis loss and reconciliation ----------

TEST_F(SchedulerTest, FirstTickPopulatesRedisFromPostgres) {
  Ready(JobPriority::kHigh, -1s);        // READY from creation: Redis has never heard of it
  Ready(JobPriority::kLow, -1s);
  Pending(JobPriority::kNormal, 60s);    // delayed
  Pending(JobPriority::kNormal, 90s);
  auto scheduler = MakeScheduler();

  const auto result = scheduler->Tick();
  EXPECT_TRUE(result.reconcile.ran);
  EXPECT_EQ(result.reconcile.ready_added, 2u);
  EXPECT_EQ(result.reconcile.delayed_added, 2u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kLow), 1u);
  EXPECT_EQ(queue_->DelayedCount(), 2u);
  EXPECT_TRUE(queue_->HasSentinel());
}

TEST_F(SchedulerTest, FlushingRedisIsRepairedFromPostgresWithoutLosingAnyJob) {
  std::vector<SchedulingEntry> ready, pending;
  for (int i = 0; i < 6; ++i) ready.push_back(Ready(kPrioritiesHighestFirst[static_cast<std::size_t>(i % 4)], -1s));
  for (int i = 0; i < 4; ++i) pending.push_back(Pending(JobPriority::kNormal, std::chrono::seconds(60 + i)));
  auto scheduler = MakeScheduler();
  scheduler->Tick();
  ASSERT_EQ(TotalReady(), 6u);
  ASSERT_EQ(queue_->DelayedCount(), 4u);
  const auto versions_before = Sql("SELECT sum(version) FROM jobs").Value(0, 0);

  redis_->Execute({"FLUSHDB"});  // Redis loses everything
  ASSERT_EQ(TotalReady(), 0u);
  ASSERT_EQ(queue_->DelayedCount(), 0u);

  const auto result = scheduler->Tick();  // notices the missing sentinel and reconciles
  EXPECT_TRUE(result.reconcile.ran);
  EXPECT_EQ(TotalReady(), 6u);
  EXPECT_EQ(queue_->DelayedCount(), 4u);
  EXPECT_EQ(result.reconcile.ready_added, 6u);
  EXPECT_EQ(result.reconcile.delayed_added, 4u);
  // The authoritative state was not modified by the repair.
  EXPECT_EQ(Sql("SELECT sum(version) FROM jobs").Value(0, 0), versions_before);
  EXPECT_EQ(Count("jobs", "status = 'READY'"), 6);
  EXPECT_EQ(Count("jobs", "status = 'PENDING'"), 4);

  // Every job is poppable again, in the right order.
  std::size_t popped = 0;
  while (queue_->PopNext(0us)) ++popped;
  EXPECT_EQ(popped, 6u);
}

TEST_F(SchedulerTest, ReconciliationRemovesStaleAndMisplacedRedisEntries) {
  const auto valid = Ready(JobPriority::kHigh, -1s);
  const auto completed = Ready(JobPriority::kNormal, -1s);
  repo_->CreateAttempt(completed.id);
  repo_->StoreResult(completed.id, 1, AttemptOutcome{}, JobState::kCompleted);
  const auto cancelled = Pending(JobPriority::kLow, 60s);
  repo_->UpdateJobState(cancelled.id, JobState::kCancelled);
  const auto wrong_queue = Ready(JobPriority::kNormal, -1s);
  const auto still_waiting = Pending(JobPriority::kNormal, 60s);

  queue_->AddReady({valid});
  // Redis contains: a COMPLETED job, a job in the wrong priority queue, a CANCELLED job in the
  // delayed set, a READY job wrongly in the delayed set, a job PostgreSQL has never heard of,
  // and a malformed member.
  auto completed_stale = completed;
  completed_stale.state = JobState::kReady;
  queue_->AddReady({completed_stale});
  auto misplaced = wrong_queue;
  misplaced.priority = JobPriority::kCritical;
  queue_->AddReady({misplaced});
  queue_->AddDelayed({cancelled, valid, still_waiting});
  auto ghost = valid;
  ghost.id = JobId::Parse("99999999-9999-9999-9999-999999999999");
  ghost.sequence = 424242;
  queue_->AddReady({ghost});
  redis_->Execute({"ZADD", queue_->ReadyKey(JobPriority::kLow), "1", "not-a-valid-member"});

  auto scheduler = MakeScheduler();
  const auto report = scheduler->Reconcile();
  ASSERT_TRUE(report.ran);

  // Exactly the READY jobs are queued under their own priority; exactly the waiting jobs are delayed.
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 1u);   // wrong_queue, at its real priority
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kCritical), 0u);  // misplaced entry gone
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kLow), 0u);       // malformed member gone
  EXPECT_EQ(TotalReady(), 2u);
  const auto delayed = queue_->DelayedMembers(0, 10);
  ASSERT_EQ(delayed.size(), 1u);
  EXPECT_EQ(delayed[0].first, ScheduleQueue::MakeMember(still_waiting));
  EXPECT_GE(report.ready_removed, 4u);
  EXPECT_GE(report.delayed_removed, 2u);

  // PostgreSQL won; nothing there changed.
  EXPECT_EQ(StateOf(completed), JobState::kCompleted);
  EXPECT_EQ(StateOf(cancelled), JobState::kCancelled);

  // A second pass finds nothing left to repair.
  EXPECT_EQ(scheduler->Reconcile().repairs(), 0u);
}

TEST_F(SchedulerTest, ReconciliationCorrectsAWrongScore) {
  const auto job = Pending(JobPriority::kNormal, 60s);
  auto skewed = job;
  skewed.scheduled_at = job.scheduled_at + 1h;
  queue_->AddDelayed({skewed});  // same member, wrong score
  auto scheduler = MakeScheduler();
  EXPECT_EQ(scheduler->Reconcile().delayed_added, 1u);  // re-added with the PostgreSQL score
  EXPECT_EQ(queue_->DelayedMembers(0, 10).at(0).second, ScheduleQueue::ScoreOf(job));
}

TEST_F(SchedulerTest, ReconciliationPagesThroughMoreJobsThanOneBatch) {
  scheduler_config_.scan_batch_size = 7;
  for (int i = 0; i < 40; ++i) Ready(JobPriority::kNormal, -1s);
  for (int i = 0; i < 25; ++i) Pending(JobPriority::kLow, std::chrono::seconds(100 + i));
  auto scheduler = MakeScheduler();
  const auto report = scheduler->Reconcile();
  EXPECT_EQ(report.ready_added, 40u);
  EXPECT_EQ(report.delayed_added, 25u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 40u);
  EXPECT_EQ(queue_->DelayedCount(), 25u);
  EXPECT_EQ(scheduler->Reconcile().repairs(), 0u);
}

TEST_F(SchedulerTest, ReconciliationYieldsToAnotherSchedulerHoldingTheLock) {
  Ready(JobPriority::kNormal, -1s);
  auto scheduler = MakeScheduler("scheduler-a");
  ASSERT_TRUE(queue_->TryLock("reconcile", "scheduler-b", 5s));
  EXPECT_FALSE(scheduler->Reconcile().ran);
  EXPECT_EQ(TotalReady(), 0u);
  queue_->Unlock("reconcile", "scheduler-b");
  EXPECT_TRUE(scheduler->Reconcile().ran);
  EXPECT_EQ(TotalReady(), 1u);
}

TEST_F(SchedulerTest, AnAbandonedReconcileLockExpiresAndDoesNotBlockTheScheduler) {
  Ready(JobPriority::kNormal, -1s);
  scheduler_config_.lock_ttl_ms = 150;
  ASSERT_TRUE(queue_->TryLock("reconcile", "crashed-scheduler", 150ms));
  auto scheduler = MakeScheduler();
  EXPECT_FALSE(scheduler->Reconcile().ran);
  std::this_thread::sleep_for(300ms);
  EXPECT_TRUE(scheduler->Reconcile().ran);
  EXPECT_EQ(TotalReady(), 1u);
}

TEST_F(SchedulerTest, ReconciliationReleasesItsLockWhenDone) {
  auto scheduler = MakeScheduler();
  scheduler->Reconcile();
  EXPECT_TRUE(queue_->TryLock("reconcile", "someone-else", 5s));  // not left held
}

TEST_F(SchedulerTest, PostgresStillPromotesWhenRedisIsDownAndRedisCatchesUpLater) {
  const auto job = Pending(JobPriority::kHigh, -1s);

  RedisConfig unreachable = config_.redis;
  unreachable.port = 1;
  unreachable.connect_timeout_ms = 200;
  RedisClient broken_redis(unreachable);
  ScheduleQueue broken_queue(broken_redis, prefix_);
  Scheduler degraded(*repo_, broken_queue, scheduler_config_, [this] { return now_; }, "degraded");

  const auto result = degraded.Tick();  // must not throw
  EXPECT_FALSE(result.redis_ok);
  EXPECT_EQ(result.promoted, 1u);
  EXPECT_EQ(StateOf(job), JobState::kReady);  // PostgreSQL stayed authoritative and consistent
  EXPECT_EQ(TotalReady(), 0u);                 // the real Redis never saw it

  auto recovered = MakeScheduler("recovered");  // Redis is back: reconciliation publishes the job
  const auto next = recovered->Tick();
  EXPECT_TRUE(next.redis_ok);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  EXPECT_EQ(queue_->PopNext(0us)->sequence, job.sequence);
}

TEST_F(SchedulerTest, PublishAddsReadyAndWaitingJobsToTheRightStructures) {
  const auto ready = Ready(JobPriority::kHigh, -1s);
  const auto waiting = Pending(JobPriority::kLow, 60s);
  const auto cancelled = Pending(JobPriority::kLow, 60s);
  repo_->UpdateJobState(cancelled.id, JobState::kCancelled);
  auto cancelled_entry = repo_->GetSchedulingEntries({cancelled.id}).front();

  auto scheduler = MakeScheduler();
  scheduler->Publish({ready, waiting, cancelled_entry});
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  EXPECT_EQ(queue_->DelayedCount(), 1u);  // the cancelled job is ignored
}

// ---------- several schedulers at once ----------

TEST_F(SchedulerTest, FiveSchedulersPromoteEveryJobExactlyOnce) {
  constexpr int kJobs = 100;
  constexpr int kSchedulers = 5;
  for (int i = 0; i < kJobs; ++i) {
    Pending(kPrioritiesHighestFirst[static_cast<std::size_t>(i % 4)], Ms(-1000 + i));
  }
  scheduler_config_.max_promotions_per_tick = 7;  // small batches force the schedulers to interleave
  scheduler_config_.reconcile_interval_ticks = 3;

  std::vector<std::unique_ptr<Scheduler>> schedulers;
  for (int i = 0; i < kSchedulers; ++i) schedulers.push_back(MakeScheduler("scheduler-" + std::to_string(i)));

  std::atomic<int> promoted_total{0};
  std::atomic<int> errors{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  for (auto& scheduler : schedulers) {
    threads.emplace_back([&, s = scheduler.get()] {
      while (!go.load()) std::this_thread::yield();
      try {
        for (int round = 0; round < 60; ++round) {
          promoted_total += static_cast<int>(s->Tick().promoted);
          if (repo_->FindDueJobs(now_, 1).empty()) break;  // nothing left to promote
        }
      } catch (...) {
        ++errors;
      }
    });
  }
  go.store(true);
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(errors.load(), 0);
  EXPECT_EQ(promoted_total.load(), kJobs) << "every job promoted, none twice";
  EXPECT_EQ(Count("jobs", "status = 'READY'"), kJobs);
  EXPECT_EQ(Count("jobs", "status <> 'READY'"), 0) << "no corrupted states";
  EXPECT_EQ(Count("jobs", "version = 2"), kJobs) << "each row was updated exactly once";
  EXPECT_EQ(Count("job_events", "to_status = 'READY'"), kJobs);
  EXPECT_EQ(Sql("SELECT count(DISTINCT job_id) FROM job_events WHERE to_status = 'READY'").Value(0, 0),
            std::to_string(kJobs));

  // Redis converged to exactly the READY set, with no duplicates (a final repair if one raced).
  schedulers[0]->Reconcile();
  EXPECT_EQ(TotalReady(), static_cast<std::size_t>(kJobs));
  EXPECT_EQ(queue_->DelayedCount(), 0u);
  std::set<std::int64_t> popped;
  while (const auto next = queue_->PopNext(0us)) popped.insert(next->sequence);
  EXPECT_EQ(popped.size(), static_cast<std::size_t>(kJobs));
}

// ---------- background loop ----------

TEST_F(SchedulerTest, TheBackgroundLoopPromotesJobsAndStopsPromptly) {
  scheduler_config_.tick_interval_ms = 10'000;  // a long wait that Stop() must interrupt
  auto scheduler = MakeScheduler();
  const auto job = Pending(JobPriority::kHigh, -1s);

  scheduler->Start();
  EXPECT_TRUE(scheduler->running());
  for (int i = 0; i < 100 && StateOf(job) != JobState::kReady; ++i) std::this_thread::sleep_for(20ms);
  EXPECT_EQ(StateOf(job), JobState::kReady);

  const auto started = std::chrono::steady_clock::now();
  scheduler->Stop();
  EXPECT_LT(std::chrono::steady_clock::now() - started, 1s) << "graceful shutdown must not wait out the tick interval";
  EXPECT_FALSE(scheduler->running());
  scheduler->Stop();  // idempotent
}

TEST_F(SchedulerTest, TicksRunAtTheConfiguredInterval) {
  scheduler_config_.tick_interval_ms = 50;
  auto scheduler = MakeScheduler();
  scheduler->Start();
  std::this_thread::sleep_for(600ms);
  scheduler->Stop();
  EXPECT_GE(scheduler->ticks(), 5u);   // ~12 ideal; ticks also take time
  EXPECT_LE(scheduler->ticks(), 14u);  // never a busy loop
}

TEST_F(SchedulerTest, ALoopThatKeepsRunningPicksUpJobsAddedLater) {
  auto scheduler = MakeScheduler();
  scheduler->Start();
  std::this_thread::sleep_for(100ms);
  const auto late = Pending(JobPriority::kCritical, -1s);
  for (int i = 0; i < 100 && StateOf(late) != JobState::kReady; ++i) std::this_thread::sleep_for(20ms);
  EXPECT_EQ(StateOf(late), JobState::kReady);
  scheduler->Stop();
}

TEST_F(SchedulerTest, SchedulerCanBeRestartedAndStopsWhenDestroyed) {
  {
    auto scheduler = MakeScheduler();
    scheduler->Start();
    scheduler->Start();  // idempotent
    scheduler->Stop();
    scheduler->Start();
    EXPECT_TRUE(scheduler->running());
  }  // destructor joins the thread
  SUCCEED();
}

}  // namespace
}  // namespace taskflow
