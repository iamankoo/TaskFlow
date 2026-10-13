#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <thread>

#include "scheduler_fixture.hpp"
#include "taskflow/scheduler/priority_queue.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;
using testing::SchedulerFixture;

// Redis-only tests: entries are built in memory, no PostgreSQL rows are involved.
class ScheduleQueueTest : public SchedulerFixture {
 protected:
  SchedulingEntry Entry(std::int64_t sequence, JobPriority priority, Ms offset = Ms(0)) {
    char buffer[40];
    std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0000-%012lld", static_cast<long long>(sequence));
    SchedulingEntry entry;
    entry.id = JobId::Parse(buffer);
    entry.sequence = sequence;
    entry.priority = priority;
    entry.state = JobState::kReady;
    entry.scheduled_at = base_ + offset;
    return entry;
  }

  std::vector<std::int64_t> DrainSequences(std::chrono::microseconds aging = 0us) {
    std::vector<std::int64_t> order;
    while (const auto popped = queue_->PopNext(aging)) order.push_back(popped->sequence);
    return order;
  }
};

TEST_F(ScheduleQueueTest, KeysAreNamespacedByPrefixAndPriority) {
  EXPECT_EQ(queue_->ReadyKey(JobPriority::kCritical), prefix_ + ":ready:CRITICAL");
  EXPECT_EQ(queue_->ReadyKey(JobPriority::kLow), prefix_ + ":ready:LOW");
  EXPECT_EQ(queue_->DelayedKey(), prefix_ + ":delayed");
  EXPECT_EQ(queue_->LockKey("reconcile"), prefix_ + ":lock:reconcile");
}

TEST_F(ScheduleQueueTest, AddReadyPlacesJobsInTheirPriorityQueueAndIsIdempotent) {
  queue_->AddReady({Entry(1, JobPriority::kHigh), Entry(2, JobPriority::kLow), Entry(3, JobPriority::kHigh)});
  queue_->AddReady({Entry(1, JobPriority::kHigh)});  // again
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 2u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kLow), 1u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kCritical), 0u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 0u);
}

TEST_F(ScheduleQueueTest, QueueStoresIdentifiersAndSortKeysNotJobData) {
  queue_->AddReady({Entry(5, JobPriority::kNormal)});
  const auto members = queue_->ReadyMembers(JobPriority::kNormal, 0, 10);
  ASSERT_EQ(members.size(), 1u);
  EXPECT_EQ(members[0].first, ScheduleQueue::MakeMember(Entry(5, JobPriority::kNormal)));
  EXPECT_LE(members[0].first.size(), 60u);  // "<20 digits>:<uuid>": nothing else is stored
  EXPECT_EQ(members[0].second, ScheduleQueue::ScoreOf(Entry(5, JobPriority::kNormal)));
}

TEST_F(ScheduleQueueTest, PopNextServesCriticalHighNormalLowThenEmpty) {
  queue_->AddReady({Entry(1, JobPriority::kLow), Entry(2, JobPriority::kNormal),
                    Entry(3, JobPriority::kCritical), Entry(4, JobPriority::kHigh)});
  const auto first = queue_->PopNext(0us);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->sequence, 3);
  EXPECT_EQ(first->priority, JobPriority::kCritical);
  EXPECT_EQ(first->scheduled_micros, ScheduleQueue::ScoreOf(Entry(3, JobPriority::kCritical)));
  EXPECT_EQ(DrainSequences(), (std::vector<std::int64_t>{4, 2, 1}));
  EXPECT_FALSE(queue_->PopNext(0us).has_value());
  EXPECT_EQ(TotalReady(), 0u);
}

TEST_F(ScheduleQueueTest, EqualPriorityIsFifoEvenWhenScoresTie) {
  // Same scheduled_at for all: only the zero-padded sequence in the member decides.
  std::vector<SchedulingEntry> entries;
  for (const std::int64_t sequence : {12, 9, 100, 10, 11}) entries.push_back(Entry(sequence, JobPriority::kHigh));
  queue_->AddReady(entries);
  EXPECT_EQ(DrainSequences(), (std::vector<std::int64_t>{9, 10, 11, 12, 100}));
}

TEST_F(ScheduleQueueTest, EarlierScheduledTimeIsServedFirstWithinAPriority) {
  queue_->AddReady({Entry(1, JobPriority::kNormal, 5s), Entry(2, JobPriority::kNormal, 0s)});
  EXPECT_EQ(DrainSequences(), (std::vector<std::int64_t>{2, 1}));
}

TEST_F(ScheduleQueueTest, AgingLetsAnOldLowJobOvertakeInRedisToo) {
  const std::chrono::microseconds aging = 60s;
  queue_->AddReady({Entry(1, JobPriority::kLow, -300s), Entry(2, JobPriority::kCritical, 0s)});
  EXPECT_EQ(DrainSequences(aging), (std::vector<std::int64_t>{1, 2}));  // LOW waited 5 levels' worth

  queue_->AddReady({Entry(1, JobPriority::kLow, -300s), Entry(2, JobPriority::kCritical, 0s)});
  EXPECT_EQ(DrainSequences(0us), (std::vector<std::int64_t>{2, 1}));  // strict priority
}

// The Lua pop and the C++ heap must implement exactly the same order.
TEST_F(ScheduleQueueTest, RedisPopOrderMatchesTheCppPriorityPolicyOnRandomData) {
  for (const auto aging : {0us, 30'000'000us}) {
    redis_->Execute({"FLUSHDB"});
    std::mt19937_64 random(1234);
    std::vector<SchedulingEntry> entries;
    for (std::int64_t i = 1; i <= 300; ++i) {
      entries.push_back(Entry(i, kPrioritiesHighestFirst[random() % 4], Ms(random() % 90'000)));
    }
    queue_->AddReady(entries);

    const PriorityPolicy policy(aging);
    std::sort(entries.begin(), entries.end(),
              [&](const SchedulingEntry& a, const SchedulingEntry& b) { return policy.Before(a, b); });
    std::vector<std::int64_t> expected;
    for (const auto& e : entries) expected.push_back(e.sequence);
    EXPECT_EQ(DrainSequences(aging), expected) << "aging=" << aging.count();
  }
}

TEST_F(ScheduleQueueTest, DelayedSetHoldsWaitingJobsAndMarkReadyMovesThemAtomically) {
  const auto a = Entry(1, JobPriority::kHigh, 10s);
  const auto b = Entry(2, JobPriority::kLow, 20s);
  queue_->AddDelayed({a, b});
  EXPECT_EQ(queue_->DelayedCount(), 2u);
  EXPECT_EQ(TotalReady(), 0u);

  queue_->MarkReady({a});
  EXPECT_EQ(queue_->DelayedCount(), 1u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  EXPECT_EQ(queue_->DelayedMembers(0, 10).front().first, ScheduleQueue::MakeMember(b));

  queue_->MarkReady({a});  // idempotent
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kHigh), 1u);
  queue_->MarkReady({Entry(3, JobPriority::kNormal)});  // never in the delayed set: still queued
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 1u);
}

TEST_F(ScheduleQueueTest, RemoveDeletesOnlyTheNamedMembers) {
  const auto a = Entry(1, JobPriority::kHigh);
  const auto b = Entry(2, JobPriority::kHigh);
  queue_->AddReady({a, b});
  queue_->RemoveReady(JobPriority::kHigh, {ScheduleQueue::MakeMember(a)});
  EXPECT_EQ(DrainSequences(), (std::vector<std::int64_t>{2}));
  queue_->AddDelayed({a, b});
  queue_->RemoveDelayed({ScheduleQueue::MakeMember(b)});
  EXPECT_EQ(queue_->DelayedCount(), 1u);
  queue_->RemoveDelayed({});  // nothing to do
}

TEST_F(ScheduleQueueTest, LargeBatchesAreChunkedAndPageable) {
  std::vector<SchedulingEntry> entries;
  for (std::int64_t i = 1; i <= 1250; ++i) entries.push_back(Entry(i, JobPriority::kNormal, Ms(i)));
  queue_->AddReady(entries);
  queue_->AddDelayed(entries);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 1250u);
  EXPECT_EQ(queue_->DelayedCount(), 1250u);

  std::size_t seen = 0;
  for (std::size_t offset = 0;; offset += 400) {
    const auto page = queue_->ReadyMembers(JobPriority::kNormal, offset, 400);
    seen += page.size();
    if (page.size() < 400) break;
  }
  EXPECT_EQ(seen, 1250u);
  queue_->MarkReady(entries);  // 1250 moves across three script calls
  EXPECT_EQ(queue_->DelayedCount(), 0u);
  EXPECT_EQ(queue_->ReadyCount(JobPriority::kNormal), 1250u);
}

TEST_F(ScheduleQueueTest, FlushingRedisLosesAllQueueState) {
  queue_->AddReady({Entry(1, JobPriority::kHigh)});
  queue_->AddDelayed({Entry(2, JobPriority::kLow)});
  queue_->SetSentinel();
  redis_->Execute({"FLUSHDB"});
  EXPECT_EQ(TotalReady(), 0u);
  EXPECT_EQ(queue_->DelayedCount(), 0u);
  EXPECT_FALSE(queue_->HasSentinel());  // how the scheduler notices
}

TEST_F(ScheduleQueueTest, SentinelMarksAReconciledRedis) {
  EXPECT_FALSE(queue_->HasSentinel());
  queue_->SetSentinel();
  EXPECT_TRUE(queue_->HasSentinel());
}

TEST_F(ScheduleQueueTest, LocksAreExclusiveOwnerCheckedAndExpire) {
  EXPECT_TRUE(queue_->TryLock("job", "alice", 5s));
  EXPECT_FALSE(queue_->TryLock("job", "bob", 5s));      // held
  queue_->Unlock("job", "bob");                          // wrong owner: no effect
  EXPECT_FALSE(queue_->TryLock("job", "bob", 5s));
  queue_->Unlock("job", "alice");
  EXPECT_TRUE(queue_->TryLock("job", "bob", 5s));       // free again
  EXPECT_TRUE(queue_->TryLock("other", "alice", 5s));   // locks are independent
}

TEST_F(ScheduleQueueTest, AnExpiredLockIsRecoverableAndOldOwnersCannotReleaseTheNewHolder) {
  EXPECT_TRUE(queue_->TryLock("job", "crashed", 150ms));
  std::this_thread::sleep_for(300ms);  // the holder died; the TTL frees the lock by itself
  EXPECT_TRUE(queue_->TryLock("job", "successor", 5s));
  queue_->Unlock("job", "crashed");  // late release by the dead owner must not free the new lock
  EXPECT_FALSE(queue_->TryLock("job", "third", 5s));
}

}  // namespace
}  // namespace taskflow
