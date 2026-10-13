#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <random>
#include <set>
#include <vector>

#include "taskflow/common/errors.hpp"
#include "taskflow/scheduler/priority_queue.hpp"
#include "taskflow/scheduler/schedule_queue.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;

const TimePoint kBase = TimePoint(std::chrono::seconds(1'800'000'000));

JobId IdFor(std::int64_t sequence) {
  char buffer[40];
  std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0000-%012lld", static_cast<long long>(sequence));
  return JobId::Parse(buffer);
}

SchedulingEntry Entry(std::int64_t sequence, JobPriority priority, std::chrono::milliseconds offset = 0ms) {
  SchedulingEntry entry;
  entry.id = IdFor(sequence);
  entry.sequence = sequence;
  entry.priority = priority;
  entry.state = JobState::kReady;
  entry.scheduled_at = kBase + offset;
  return entry;
}

std::vector<std::int64_t> Drain(JobPriorityQueue& queue) {
  std::vector<std::int64_t> order;
  while (auto next = queue.Pop()) order.push_back(next->sequence);
  return order;
}

// ---------- ordering ----------

TEST(PriorityQueueTest, CriticalBeforeHighBeforeNormalBeforeLow) {
  JobPriorityQueue queue;
  queue.Push(Entry(1, JobPriority::kLow));
  queue.Push(Entry(2, JobPriority::kNormal));
  queue.Push(Entry(3, JobPriority::kCritical));
  queue.Push(Entry(4, JobPriority::kHigh));
  EXPECT_EQ(Drain(queue), (std::vector<std::int64_t>{3, 4, 2, 1}));
}

TEST(PriorityQueueTest, EqualPriorityIsFifoBySequence) {
  JobPriorityQueue queue;
  for (const std::int64_t sequence : {12, 10, 11}) queue.Push(Entry(sequence, JobPriority::kHigh));
  EXPECT_EQ(Drain(queue), (std::vector<std::int64_t>{10, 11, 12}));  // #10 -> #11 -> #12
}

TEST(PriorityQueueTest, EarlierScheduledTimeBeatsLowerSequenceAtEqualPriority) {
  JobPriorityQueue queue;
  queue.Push(Entry(1, JobPriority::kNormal, 5s));  // submitted first but due later
  queue.Push(Entry(2, JobPriority::kNormal, 0s));
  EXPECT_EQ(Drain(queue), (std::vector<std::int64_t>{2, 1}));
}

TEST(PriorityQueueTest, EmptyQueueBehaves) {
  JobPriorityQueue queue;
  EXPECT_TRUE(queue.empty());
  EXPECT_EQ(queue.size(), 0u);
  EXPECT_EQ(queue.Top(), nullptr);
  EXPECT_FALSE(queue.Pop().has_value());
  EXPECT_FALSE(queue.Remove(IdFor(1)));
  EXPECT_FALSE(queue.Contains(IdFor(1)));
  EXPECT_EQ(queue.RemoveIf([](const SchedulingEntry&) { return true; }), 0u);
}

TEST(PriorityQueueTest, TopIsTheNextPopWithoutRemovingIt) {
  JobPriorityQueue queue;
  queue.Push(Entry(1, JobPriority::kLow));
  queue.Push(Entry(2, JobPriority::kCritical));
  ASSERT_NE(queue.Top(), nullptr);
  EXPECT_EQ(queue.Top()->sequence, 2);
  EXPECT_EQ(queue.size(), 2u);
  EXPECT_EQ(queue.Pop()->sequence, 2);
  EXPECT_EQ(queue.Top()->sequence, 1);
}

// ---------- duplicates, removal, stale entries ----------

TEST(PriorityQueueTest, PushingTheSameJobTwiceKeepsOneEntry) {
  JobPriorityQueue queue;
  EXPECT_TRUE(queue.Push(Entry(1, JobPriority::kLow)));
  EXPECT_FALSE(queue.Push(Entry(1, JobPriority::kLow)));  // duplicate
  EXPECT_EQ(queue.size(), 1u);
  EXPECT_EQ(Drain(queue), (std::vector<std::int64_t>{1}));
}

TEST(PriorityQueueTest, RePushingReplacesAndResortsTheEntry) {
  JobPriorityQueue queue;
  queue.Push(Entry(1, JobPriority::kLow));
  queue.Push(Entry(2, JobPriority::kNormal));
  queue.Push(Entry(3, JobPriority::kNormal));
  EXPECT_FALSE(queue.Push(Entry(1, JobPriority::kCritical)));  // priority edited upwards
  EXPECT_EQ(queue.Top()->sequence, 1);
  EXPECT_FALSE(queue.Push(Entry(1, JobPriority::kLow)));  // and back down
  EXPECT_EQ(Drain(queue), (std::vector<std::int64_t>{2, 3, 1}));
}

TEST(PriorityQueueTest, RemoveInvalidatesAnArbitraryEntry) {
  JobPriorityQueue queue;
  for (std::int64_t i = 1; i <= 20; ++i) queue.Push(Entry(i, JobPriority::kNormal));
  EXPECT_TRUE(queue.Remove(IdFor(7)));
  EXPECT_TRUE(queue.Remove(IdFor(1)));   // the current top
  EXPECT_TRUE(queue.Remove(IdFor(20)));  // the last
  EXPECT_FALSE(queue.Remove(IdFor(7)));  // already gone
  EXPECT_FALSE(queue.Contains(IdFor(7)));
  EXPECT_EQ(queue.size(), 17u);
  const auto order = Drain(queue);
  EXPECT_TRUE(std::is_sorted(order.begin(), order.end()));
  EXPECT_EQ(std::count(order.begin(), order.end(), 7), 0);
}

TEST(PriorityQueueTest, RemoveIfDropsStaleEntriesAndKeepsHeapOrder) {
  JobPriorityQueue queue;
  for (std::int64_t i = 1; i <= 30; ++i) {
    queue.Push(Entry(i, i % 2 == 0 ? JobPriority::kHigh : JobPriority::kLow));
  }
  const auto removed = queue.RemoveIf([](const SchedulingEntry& e) { return e.sequence % 3 == 0; });
  EXPECT_EQ(removed, 10u);
  EXPECT_EQ(queue.size(), 20u);
  const auto order = Drain(queue);
  ASSERT_EQ(order.size(), 20u);
  EXPECT_EQ(order.front() % 2, 0);  // HIGH entries first
  for (const auto sequence : order) EXPECT_NE(sequence % 3, 0);
}

TEST(PriorityQueueTest, ClearEmptiesTheQueue) {
  JobPriorityQueue queue;
  queue.Push(Entry(1, JobPriority::kHigh));
  queue.Clear();
  EXPECT_TRUE(queue.empty());
  EXPECT_FALSE(queue.Contains(IdFor(1)));
  EXPECT_TRUE(queue.Push(Entry(1, JobPriority::kHigh)));  // can be queued again
}

// ---------- scale and correctness against a reference ----------

TEST(PriorityQueueTest, LargeQueuePopsInExactlySortedOrder) {
  std::mt19937_64 random(42);
  const PriorityPolicy policy;
  std::vector<SchedulingEntry> entries;
  for (std::int64_t i = 1; i <= 50'000; ++i) {
    entries.push_back(Entry(i, kPrioritiesHighestFirst[random() % 4],
                            std::chrono::milliseconds(random() % 1000)));
  }
  JobPriorityQueue queue(policy);
  std::vector<SchedulingEntry> shuffled = entries;
  std::shuffle(shuffled.begin(), shuffled.end(), random);
  for (const auto& entry : shuffled) queue.Push(entry);
  ASSERT_EQ(queue.size(), 50'000u);

  std::sort(entries.begin(), entries.end(),
            [&](const SchedulingEntry& a, const SchedulingEntry& b) { return policy.Before(a, b); });
  for (const auto& expected : entries) {
    const auto popped = queue.Pop();
    ASSERT_TRUE(popped.has_value());
    ASSERT_EQ(popped->sequence, expected.sequence);
  }
  EXPECT_TRUE(queue.empty());
}

TEST(PriorityQueueTest, RandomizedOperationsMatchAReferenceModel) {
  for (const auto aging : {0us, 30'000'000us}) {
    std::mt19937_64 random(7);
    const PriorityPolicy policy(aging);
    JobPriorityQueue queue(policy);
    auto before = [&](const SchedulingEntry& a, const SchedulingEntry& b) { return policy.Before(a, b); };
    std::set<SchedulingEntry, decltype(before)> reference(before);
    std::int64_t next_sequence = 1;

    for (int step = 0; step < 20'000; ++step) {
      const auto action = random() % 10;
      if (action < 5 || reference.empty()) {
        const auto entry = Entry(next_sequence++, kPrioritiesHighestFirst[random() % 4],
                                 std::chrono::milliseconds(random() % 120'000));
        queue.Push(entry);
        reference.insert(entry);
      } else if (action < 8) {
        const auto expected = *reference.begin();
        reference.erase(reference.begin());
        const auto popped = queue.Pop();
        ASSERT_TRUE(popped.has_value());
        ASSERT_EQ(popped->sequence, expected.sequence) << "aging=" << aging.count() << " step=" << step;
      } else {
        auto victim = reference.begin();
        std::advance(victim, static_cast<long>(random() % reference.size()));
        ASSERT_TRUE(queue.Remove(victim->id));
        reference.erase(victim);
      }
      ASSERT_EQ(queue.size(), reference.size());
    }
  }
}

// ---------- priority policy and aging ----------

TEST(PriorityPolicyTest, LevelsAreOrderedAndCentral) {
  EXPECT_GT(PriorityPolicy::Level(JobPriority::kCritical), PriorityPolicy::Level(JobPriority::kHigh));
  EXPECT_GT(PriorityPolicy::Level(JobPriority::kHigh), PriorityPolicy::Level(JobPriority::kNormal));
  EXPECT_GT(PriorityPolicy::Level(JobPriority::kNormal), PriorityPolicy::Level(JobPriority::kLow));
  EXPECT_EQ(kPrioritiesHighestFirst.front(), JobPriority::kCritical);
  EXPECT_EQ(kPrioritiesHighestFirst.back(), JobPriority::kLow);
}

TEST(PriorityPolicyTest, WithoutAgingPriorityAlwaysWins) {
  const PriorityPolicy strict;  // aging disabled
  EXPECT_FALSE(strict.aging_enabled());
  const auto ancient_low = Entry(1, JobPriority::kLow, -std::chrono::hours(24 * 365));
  const auto fresh_critical = Entry(2, JobPriority::kCritical, 0ms);
  EXPECT_TRUE(strict.Before(fresh_critical, ancient_low));
  EXPECT_FALSE(strict.Before(ancient_low, fresh_critical));
}

TEST(PriorityPolicyTest, AgingLetsALongWaitingLowJobOvertake) {
  const PriorityPolicy aging(60s);  // one level per minute of waiting
  const auto fresh_critical = Entry(2, JobPriority::kCritical, 0s);

  // LOW is three levels below CRITICAL: after 3 minutes it ties, after more it overtakes.
  const auto low_waited_2min = Entry(1, JobPriority::kLow, -120s);
  const auto low_waited_3min = Entry(3, JobPriority::kLow, -180s);
  const auto low_waited_4min = Entry(4, JobPriority::kLow, -240s);
  EXPECT_TRUE(aging.Before(fresh_critical, low_waited_2min));
  EXPECT_TRUE(aging.Before(low_waited_3min, fresh_critical) || aging.Before(fresh_critical, low_waited_3min));
  EXPECT_TRUE(aging.Before(low_waited_4min, fresh_critical));
}

TEST(PriorityPolicyTest, HigherPriorityStillWinsWithinTheAgingInterval) {
  const PriorityPolicy aging(60s);
  const auto high = Entry(2, JobPriority::kHigh, 10s);
  const auto normal = Entry(1, JobPriority::kNormal, 0s);  // waited 10 s longer: far less than one level
  EXPECT_TRUE(aging.Before(high, normal));
  const auto normal_much_older = Entry(3, JobPriority::kNormal, -90s);  // 100 s older = 1.67 levels
  EXPECT_TRUE(aging.Before(normal_much_older, high));
}

TEST(PriorityPolicyTest, OrderIsIndependentOfTheCurrentTime) {
  // Aging uses a static key, so deciding "who goes first" never needs the clock.
  const PriorityPolicy aging(45s);
  const auto a = Entry(1, JobPriority::kLow, -100s);
  const auto b = Entry(2, JobPriority::kHigh, 0s);
  const bool a_first = aging.Before(a, b);
  for (const auto now : {kBase, kBase + 1h, kBase + 24h}) {
    const bool by_effective_level = aging.EffectiveLevel(a, now) > aging.EffectiveLevel(b, now);
    EXPECT_EQ(by_effective_level, a_first) << "now offset " << (now - kBase).count();
  }
}

TEST(PriorityPolicyTest, EffectiveLevelGrowsOneLevelPerInterval) {
  const PriorityPolicy aging(60s);
  const auto low = Entry(1, JobPriority::kLow, 0s);
  EXPECT_DOUBLE_EQ(aging.EffectiveLevel(low, kBase), 0.0);
  EXPECT_DOUBLE_EQ(aging.EffectiveLevel(low, kBase + 60s), 1.0);
  EXPECT_DOUBLE_EQ(aging.EffectiveLevel(low, kBase + 180s), 3.0);
  EXPECT_DOUBLE_EQ(aging.EffectiveLevel(low, kBase - 5s), 0.0);  // not yet waiting
}

TEST(PriorityPolicyTest, ALowJobIsEventuallyServedUnderContinuousCriticalTraffic) {
  // Simulate a scheduler that serves one job per second while a new CRITICAL job arrives
  // every second: with aging the LOW job is served within 4 intervals; without, never.
  const std::chrono::seconds interval = 20s;
  for (const bool with_aging : {true, false}) {
    JobPriorityQueue queue(PriorityPolicy(with_aging ? std::chrono::duration_cast<std::chrono::microseconds>(interval)
                                                     : std::chrono::microseconds(0)));
    queue.Push(Entry(1, JobPriority::kLow, 0s));
    std::int64_t served_low_at = -1;
    for (std::int64_t second = 1; second <= 600; ++second) {
      queue.Push(Entry(1000 + second, JobPriority::kCritical, std::chrono::seconds(second)));
      const auto next = queue.Pop();
      ASSERT_TRUE(next.has_value());
      if (next->sequence == 1) {
        served_low_at = second;
        break;
      }
    }
    if (with_aging) {
      ASSERT_GT(served_low_at, 0) << "LOW job starved despite aging";
      EXPECT_LE(served_low_at, 4 * interval.count());
    } else {
      EXPECT_EQ(served_low_at, -1) << "strict priority is expected to starve LOW here";
    }
  }
}

TEST(PriorityPolicyTest, AgingDoesNotModifyTheEntry) {
  const PriorityPolicy aging(1s);
  const auto entry = Entry(1, JobPriority::kLow, -1h);
  const auto copy = entry;
  (void)aging.AgedKey(entry);
  (void)aging.EffectiveLevel(entry, kBase);
  EXPECT_EQ(entry.priority, copy.priority);  // the user-visible priority is never rewritten
  EXPECT_EQ(entry.scheduled_at, copy.scheduled_at);
}

// ---------- Redis member encoding ----------

TEST(ScheduleQueueMemberTest, MembersRoundTripAndSortBySequence) {
  const auto entry = Entry(42, JobPriority::kHigh);
  const std::string member = ScheduleQueue::MakeMember(entry);
  EXPECT_EQ(member.size(), 20u + 1u + 36u);
  const auto parsed = ScheduleQueue::ParseMember(member);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->first, 42);
  EXPECT_EQ(parsed->second, entry.id);

  // Zero padding makes lexicographic order equal numeric order.
  EXPECT_LT(ScheduleQueue::MakeMember(Entry(9, JobPriority::kHigh)),
            ScheduleQueue::MakeMember(Entry(10, JobPriority::kHigh)));
  EXPECT_LT(ScheduleQueue::MakeMember(Entry(99, JobPriority::kHigh)),
            ScheduleQueue::MakeMember(Entry(100, JobPriority::kHigh)));
}

TEST(ScheduleQueueMemberTest, MalformedMembersAreRejected) {
  for (const char* bad : {"", "garbage", "123:00000000-0000-0000-0000-000000000001",
                          "0000000000000000004x:00000000-0000-0000-0000-000000000001",
                          "00000000000000000042:not-a-uuid-not-a-uuid-not-a-uuid-123",
                          "00000000000000000042-00000000-0000-0000-0000-000000000001"}) {
    EXPECT_FALSE(ScheduleQueue::ParseMember(bad).has_value()) << bad;
  }
}

TEST(ScheduleQueueMemberTest, ScoreIsMicrosecondsSinceTheEpoch) {
  auto entry = Entry(1, JobPriority::kLow);
  entry.scheduled_at = TimePoint(std::chrono::microseconds(1'800'000'000'123'456));
  EXPECT_EQ(ScheduleQueue::ScoreOf(entry), 1'800'000'000'123'456);
}

}  // namespace
}  // namespace taskflow
