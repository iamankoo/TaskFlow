#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <thread>

#include "dag_fixture.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;
using testing::DagFixture;

class DagSchedulerTest : public DagFixture {
 protected:
  // READY jobs the scheduler has released, in promotion order, tick by tick, until idle.
  std::vector<std::vector<std::int64_t>> Drain(Scheduler& scheduler, int max_ticks = 30) {
    std::vector<std::vector<std::int64_t>> batches;
    for (int i = 0; i < max_ticks; ++i) {
      const auto result = scheduler.Tick();
      if (result.promoted == 0) break;
      batches.push_back(Sequences(result.promoted_entries));
    }
    return batches;
  }

  // What a worker would do with a job the scheduler made READY: run it and complete it.
  void RunToCompletion(const SchedulingEntry& job) {
    const JobAttempt attempt = repo_->CreateAttempt(job.id);
    repo_->StoreResult(job.id, attempt.attempt_number, AttemptOutcome{}, JobState::kCompleted);
  }
};

TEST_F(DagSchedulerTest, JobsWithoutDependenciesScheduleExactlyAsBefore) {
  const auto low = Pending(JobPriority::kLow, Ms(-1000));
  const auto critical = Pending(JobPriority::kCritical, Ms(-1000));
  const auto future = Pending(JobPriority::kHigh, Ms(60'000));
  scheduler_config_.max_promotions_per_tick = 1;
  auto scheduler = MakeScheduler();

  EXPECT_EQ(Sequences(scheduler->Tick().promoted_entries), (std::vector<std::int64_t>{critical.sequence}));
  EXPECT_EQ(Sequences(scheduler->Tick().promoted_entries), (std::vector<std::int64_t>{low.sequence}));
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  EXPECT_EQ(State(future), JobState::kPending);  // still delayed
}

TEST_F(DagSchedulerTest, ADependentStaysBlockedWhileItsPrerequisiteIsUnfinished) {
  const auto parent = Due();
  const auto child = Due();
  Edge(child, parent);
  auto scheduler = MakeScheduler();

  // parent is PENDING and due: it is promoted; the (equally due) child is not.
  auto result = scheduler->Tick();
  EXPECT_EQ(Sequences(result.promoted_entries), (std::vector<std::int64_t>{parent.sequence}));
  EXPECT_EQ(State(child), JobState::kPending);

  EXPECT_EQ(scheduler->Tick().promoted, 0u) << "parent is READY (not finished): child still blocked";
  Start(parent);
  EXPECT_EQ(scheduler->Tick().promoted, 0u) << "parent is RUNNING: child still blocked";
  EXPECT_EQ(State(child), JobState::kPending);
  EXPECT_EQ(queue_->PopNext(0us)->sequence, parent.sequence) << "only the parent was ever queued";
  EXPECT_FALSE(queue_->PopNext(0us).has_value());
}

TEST_F(DagSchedulerTest, ACompletedPrerequisiteReleasesTheDependentExactlyOnce) {
  const auto parent = Due();
  const auto child = Due();
  Edge(child, parent);
  auto scheduler = MakeScheduler();
  scheduler->Tick();
  RunToCompletion(parent);

  const auto released = scheduler->Tick();
  EXPECT_EQ(Sequences(released.promoted_entries), (std::vector<std::int64_t>{child.sequence}));
  EXPECT_EQ(State(child), JobState::kReady);
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  EXPECT_EQ(ReadyEvents(child), 1);
  // The finished parent's queue entry is stale until the next repair pass (harmless: a worker
  // would lose the claim); after reconciliation only the released child is queued.
  scheduler->Reconcile();
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 1u);
  EXPECT_EQ(queue_->PopNext(0us)->sequence, child.sequence);
}

TEST_F(DagSchedulerTest, CompletingOneParentDoesNotReleaseAJobWhoseOtherParentsAreUnfinished) {
  const auto a = Due(), b = Due(), c = Due(), child = Due();
  deps_->AddDependencies({{child.id, a.id}, {child.id, b.id}, {child.id, c.id}});
  auto scheduler = MakeScheduler();
  Drain(*scheduler);  // promotes the three parents

  RunToCompletion(a);
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  RunToCompletion(b);
  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  EXPECT_EQ(State(child), JobState::kPending);
  RunToCompletion(c);
  EXPECT_EQ(Sequences(scheduler->Tick().promoted_entries), (std::vector<std::int64_t>{child.sequence}));
}

TEST_F(DagSchedulerTest, TheDiamondWorkflowRunsInDependencyOrder) {
  // A: fetch -> B, C -> D: report -> E: publish
  WorkflowSpec spec;
  const auto node = [this](const std::string& key, std::vector<std::string> depends_on) {
    WorkflowNode n;
    n.key = key;
    n.job.type = "wf." + key;
    n.job.scheduled_at = base_ - std::chrono::seconds(1);  // due at the fixture's fixed clock
    n.depends_on = std::move(depends_on);
    return n;
  };
  spec.nodes = {node("A", {}), node("B", {"A"}), node("C", {"A"}), node("D", {"B", "C"}), node("E", {"D"})};
  const auto created = deps_->CreateWorkflow(spec);
  const auto entry = [&](const char* key) { return repo_->GetSchedulingEntries({created.jobs.at(key).id}).front(); };
  auto scheduler = MakeScheduler();

  std::vector<std::vector<std::string>> waves;
  for (int step = 0; step < 10; ++step) {
    const auto result = scheduler->Tick();
    if (result.promoted == 0) break;
    std::vector<std::string> wave;
    for (const auto& promoted : result.promoted_entries) {
      for (const auto& [key, job] : created.jobs) {
        if (job.id == promoted.id) wave.push_back(key);
      }
      RunToCompletion(promoted);  // the worker finishes it before the next tick
    }
    std::sort(wave.begin(), wave.end());
    waves.push_back(wave);
  }
  ASSERT_EQ(waves.size(), 4u);
  EXPECT_EQ(waves[0], (std::vector<std::string>{"A"}));
  EXPECT_EQ(waves[1], (std::vector<std::string>{"B", "C"}));  // independent jobs are released together
  EXPECT_EQ(waves[2], (std::vector<std::string>{"D"}));
  EXPECT_EQ(waves[3], (std::vector<std::string>{"E"}));
  for (const char* key : {"A", "B", "C", "D", "E"}) {
    EXPECT_EQ(State(entry(key)), JobState::kCompleted) << key;
    EXPECT_EQ(ReadyEvents(entry(key)), 1) << key;
  }
}

TEST_F(DagSchedulerTest, IndependentChainsProgressIndependently) {
  const auto a1 = Due(), a2 = Due(), b1 = Due(), b2 = Due();
  Edge(a2, a1);
  Edge(b2, b1);
  auto scheduler = MakeScheduler();
  scheduler->Tick();  // promotes a1 and b1
  Finish(a1, JobState::kFailed);  // chain A fails ...
  RunToCompletion(b1);            // ... chain B proceeds
  scheduler->Tick();
  EXPECT_EQ(State(a2), JobState::kFailed);
  EXPECT_EQ(State(b2), JobState::kReady);
}

TEST_F(DagSchedulerTest, AFailedPrerequisiteNeverReleasesItsDependentsAndLeavesThemEnded) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{b.id, a.id}, {c.id, b.id}});
  auto scheduler = MakeScheduler();
  scheduler->Tick();
  Finish(a, JobState::kFailed);

  EXPECT_EQ(scheduler->Tick().promoted, 0u);
  EXPECT_EQ(State(b), JobState::kFailed);
  EXPECT_EQ(State(c), JobState::kFailed);
  EXPECT_EQ(ReadyEvents(b), 0);
  EXPECT_EQ(ReadyEvents(c), 0);
  scheduler->Reconcile();  // Redis still listed them as waiting until the next repair pass
  EXPECT_EQ(queue_->DelayedCount(), 0u) << "ended jobs are not waiting any more";
  EXPECT_EQ(TotalReady(), 0u);
}

TEST_F(DagSchedulerTest, TheDatabaseRefusesToReleaseABlockedJobEvenOnDirectRequest) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  EXPECT_THROW(repo_->UpdateJobState(b.id, JobState::kReady), ConflictError);
  EXPECT_EQ(State(b), JobState::kPending);
  EXPECT_THROW(Sql("UPDATE jobs SET status = 'READY' WHERE id = $1::uuid", {b.id.str()}), DatabaseError);
}

TEST_F(DagSchedulerTest, ADuePrerequisiteBacklogDoesNotCrowdOutRunnableJobs) {
  std::vector<SchedulingEntry> blocked;
  const auto gate = Pending(JobPriority::kNormal, Ms(60'000));  // not due for a minute: blocks everything below
  for (int i = 0; i < 5; ++i) {
    blocked.push_back(Pending(JobPriority::kCritical, Ms(-5000)));  // older and higher priority
    Edge(blocked.back(), gate);
  }
  const auto runnable_a = Due(JobPriority::kLow);
  const auto runnable_b = Due(JobPriority::kLow);
  scheduler_config_.candidates_per_priority = 2;  // the window is smaller than the blocked backlog
  auto scheduler = MakeScheduler();

  const auto result = scheduler->Tick();
  EXPECT_EQ(Sequences(result.promoted_entries),
            (std::vector<std::int64_t>{runnable_a.sequence, runnable_b.sequence}))
      << "blocked jobs are filtered out in SQL, so they cannot fill the candidate window";
}

// ---------- Redis never overrides PostgreSQL ----------

TEST_F(DagSchedulerTest, RedisLossAndReconciliationDoNotReleaseBlockedJobs) {
  const auto parent = Due(), child = Due();
  Edge(child, parent);
  auto scheduler = MakeScheduler();
  scheduler->Tick();
  redis_->Execute({"FLUSHDB"});
  scheduler->Tick();  // reconciles from PostgreSQL

  EXPECT_EQ(State(child), JobState::kPending);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 1u) << "only the parent";
  EXPECT_EQ(queue_->DelayedCount(), 1u) << "the blocked child is waiting, not ready";
  EXPECT_EQ(queue_->PopNext(0us)->sequence, parent.sequence);
  EXPECT_FALSE(queue_->PopNext(0us).has_value());
}

TEST_F(DagSchedulerTest, AStaleRedisEntryForABlockedJobIsRemovedAndNeverServed) {
  const auto parent = Due(), child = Due();
  Edge(child, parent);
  auto forged = child;
  forged.state = JobState::kReady;
  queue_->AddReady({forged});  // Redis claims the blocked child is ready

  auto scheduler = MakeScheduler();
  const auto report = scheduler->Reconcile();
  EXPECT_GE(report.ready_removed, 1u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 0u);
  EXPECT_EQ(State(child), JobState::kPending) << "PostgreSQL state was never touched by Redis";
}

// ---------- several schedulers ----------

TEST_F(DagSchedulerTest, ManySchedulersReleaseAFanOutExactlyOnce) {
  const auto root = Due();
  std::vector<SchedulingEntry> children;
  std::vector<DependencyEdge> edges;
  for (int i = 0; i < 60; ++i) {
    children.push_back(Due(kPrioritiesHighestFirst[static_cast<std::size_t>(i % 4)]));
    edges.push_back({children.back().id, root.id});
  }
  deps_->AddDependencies(edges);
  scheduler_config_.max_promotions_per_tick = 7;
  auto first = MakeScheduler("first");
  first->Tick();            // promotes the root
  RunToCompletion(root);

  std::vector<std::unique_ptr<Scheduler>> schedulers;
  for (int i = 0; i < 5; ++i) schedulers.push_back(MakeScheduler("s" + std::to_string(i)));
  std::atomic<int> promoted{0}, errors{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  for (auto& scheduler : schedulers) {
    threads.emplace_back([&, s = scheduler.get()] {
      while (!go.load()) std::this_thread::yield();
      try {
        for (int round = 0; round < 40; ++round) {
          promoted += static_cast<int>(s->Tick().promoted);
          if (repo_->FindDueJobs(now_, 1).empty()) break;
        }
      } catch (...) {
        ++errors;
      }
    });
  }
  go.store(true);
  for (auto& thread : threads) thread.join();

  EXPECT_EQ(errors.load(), 0);
  EXPECT_EQ(promoted.load(), 60);
  for (const auto& child : children) {
    ASSERT_EQ(State(child), JobState::kReady);
    ASSERT_EQ(ReadyEvents(child), 1);
    ASSERT_EQ(repo_->GetJob(child.id)->version, 2) << "released by exactly one scheduler";
  }
}

TEST_F(DagSchedulerTest, TwoParentsFinishingTogetherReleaseTheChildOnceAndOnlyAfterBoth) {
  for (int round = 0; round < 15; ++round) {
    const auto a = Due(), b = Due(), child = Due();
    deps_->AddDependencies({{child.id, a.id}, {child.id, b.id}});
    Start(a);
    Start(b);

    std::atomic<bool> stop{false};
    std::atomic<int> errors{0};
    std::vector<std::thread> schedulers;
    for (int i = 0; i < 3; ++i) {  // schedulers keep ticking while the parents finish
      schedulers.emplace_back([&] {
        try {
          auto scheduler = MakeScheduler("racer");
          while (!stop.load()) scheduler->Tick();
        } catch (...) {
          ++errors;
        }
      });
    }
    std::atomic<bool> go{false};
    const auto finish = [&](const SchedulingEntry& job) {
      return std::thread([&, job] {
        while (!go.load()) std::this_thread::yield();
        try {
          repo_->StoreResult(job.id, 1, AttemptOutcome{}, JobState::kCompleted);
        } catch (...) {
          ++errors;
        }
      });
    };
    auto t1 = finish(a);
    auto t2 = finish(b);
    go.store(true);
    t1.join();
    t2.join();
    std::this_thread::sleep_for(150ms);  // let the schedulers see the final state
    stop.store(true);
    for (auto& thread : schedulers) thread.join();

    ASSERT_EQ(errors.load(), 0) << "round " << round;
    ASSERT_EQ(State(child), JobState::kReady) << "round " << round;
    ASSERT_EQ(ReadyEvents(child), 1) << "released exactly once: round " << round;
    // Causality: the release happened after both parents completed.
    const auto released_at = std::stoll(Sql("SELECT max(id) FROM job_events WHERE to_status = 'READY' AND job_id = $1::uuid",
                                            {child.id.str()}).Value(0, 0));
    const auto last_completion = std::stoll(
        Sql("SELECT max(id) FROM job_events WHERE to_status = 'COMPLETED' AND job_id IN ($1::uuid, $2::uuid)",
            {a.id.str(), b.id.str()}).Value(0, 0));
    ASSERT_GT(released_at, last_completion) << "round " << round;
  }
}

TEST_F(DagSchedulerTest, TheSchedulersSafetyNetEndsJobsStrandedByAnOutOfBandChange) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  Sql("UPDATE jobs SET status = 'FAILED', finished_at = now() WHERE id = $1::uuid", {a.id.str()});  // bypasses propagation
  auto scheduler = MakeScheduler();
  scheduler->SetDependencyService(deps_.get());
  const auto result = scheduler->Tick();  // the first tick is a periodic one
  EXPECT_EQ(result.dependents_ended, 1u);
  EXPECT_EQ(State(b), JobState::kFailed);
}

TEST_F(DagSchedulerTest, ADelayedDependentWaitsForBothItsPrerequisitesAndItsOwnTime) {
  const auto parent = Due();
  const auto child = Pending(JobPriority::kNormal, Ms(30'000));  // due only 30 s from now
  Edge(child, parent);
  auto scheduler = MakeScheduler();
  scheduler->Tick();
  RunToCompletion(parent);

  now_ = base_ + 29s;
  EXPECT_EQ(scheduler->Tick().promoted, 0u) << "prerequisite done, but the job's own time has not come";
  now_ = base_ + 30s;
  EXPECT_EQ(scheduler->Tick().promoted, 1u);
  EXPECT_EQ(State(child), JobState::kReady);
}

}  // namespace
}  // namespace taskflow
