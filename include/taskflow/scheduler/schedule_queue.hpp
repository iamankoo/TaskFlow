#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "taskflow/jobs/job.hpp"
#include "taskflow/redis/redis_client.hpp"

namespace taskflow {

// Redis side of scheduling. Redis is a fast, rebuildable index of what PostgreSQL
// says is schedulable; it never holds job data, only identifiers and sort keys.
//
//   {prefix}:ready:{CRITICAL|HIGH|NORMAL|LOW}   sorted set per priority: READY jobs
//   {prefix}:delayed                            sorted set: PENDING / RETRY_WAIT jobs
//   {prefix}:lock:{name}                        short-lived lock (SET NX PX)
//   {prefix}:sentinel                           present once Redis was reconciled
//
// Sorted-set member:  "<sequence, 20 digits zero padded>:<job uuid>"
// Sorted-set score:   scheduled_at in microseconds since the Unix epoch
//
// Zero-padding makes equal-score members sort by sequence, so each queue is FIFO by
// (scheduled_at, sequence) with no extra bookkeeping. See docs/SCHEDULER.md.
class ScheduleQueue {
 public:
  // A job popped from a ready queue.
  struct Popped {
    JobId id;
    std::int64_t sequence = 0;
    JobPriority priority = JobPriority::kNormal;
    std::int64_t scheduled_micros = 0;
  };
  using MemberScores = std::vector<std::pair<std::string, std::int64_t>>;

  ScheduleQueue(RedisClient& redis, std::string prefix);

  // ----- key names and member encoding -----
  std::string ReadyKey(JobPriority priority) const;
  std::string DelayedKey() const;
  std::string LockKey(std::string_view name) const;
  std::string SentinelKey() const;

  static std::string MakeMember(const SchedulingEntry& entry);
  static std::int64_t ScoreOf(const SchedulingEntry& entry);
  // nullopt when `member` is not in the "<sequence>:<uuid>" form.
  static std::optional<std::pair<std::int64_t, JobId>> ParseMember(std::string_view member);

  // ----- writes (all idempotent) -----
  void AddReady(const std::vector<SchedulingEntry>& entries);
  void AddDelayed(const std::vector<SchedulingEntry>& entries);
  // Atomically removes each job from the delayed set and adds it to its ready queue.
  void MarkReady(const std::vector<SchedulingEntry>& entries);
  void RemoveReady(JobPriority priority, const std::vector<std::string>& members);
  void RemoveDelayed(const std::vector<std::string>& members);

  // Atomically removes and returns the job that should run next across all four ready
  // queues: highest priority first, FIFO within a priority, with `aging_interval` applied
  // exactly as PriorityPolicy does (0 = strict priority). Nullopt when every queue is empty.
  std::optional<Popped> PopNext(std::chrono::microseconds aging_interval);

  // ----- reads -----
  std::size_t ReadyCount(JobPriority priority);
  std::size_t DelayedCount();
  MemberScores ReadyMembers(JobPriority priority, std::size_t offset, std::size_t count);
  MemberScores DelayedMembers(std::size_t offset, std::size_t count);

  // ----- coordination -----
  // Takes a lock that expires by itself after `ttl`. False when someone else holds it.
  bool TryLock(std::string_view name, const std::string& owner, std::chrono::milliseconds ttl);
  // Releases the lock only if `owner` still holds it (it may have expired and been re-taken).
  void Unlock(std::string_view name, const std::string& owner);

  bool HasSentinel();
  void SetSentinel();

 private:
  RedisClient& redis_;
  std::string prefix_;
};

}  // namespace taskflow
