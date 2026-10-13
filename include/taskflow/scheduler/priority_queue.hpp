#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "taskflow/jobs/job.hpp"

namespace taskflow {

// The one place that defines how jobs are ordered.
//
//   * Higher priority first: CRITICAL > HIGH > NORMAL > LOW.
//   * Equal priority: earlier scheduled_at first, then lower sequence (FIFO).
//   * Aging: every `aging_interval` a job has waited counts as one extra priority
//     level, so a LOW job cannot starve behind a steady stream of CRITICAL jobs.
//     "Waited" is measured from scheduled_at, so the persisted priority never changes.
//
// Aging keeps the sort key independent of the current time. Job A outranks job B
// when  level_a + (now - t_a)/I  >  level_b + (now - t_b)/I, and `now` cancels,
// leaving the static key  t - level * I  (smaller is served first). A plain heap
// therefore stays valid as time passes. With aging disabled (interval 0) the order
// is the strict tuple (priority desc, scheduled_at, sequence).
class PriorityPolicy {
 public:
  explicit PriorityPolicy(std::chrono::microseconds aging_interval = std::chrono::microseconds(0));

  // The single mapping from a priority to its numeric level (higher = more urgent).
  static int Level(JobPriority priority) noexcept { return ToInt(priority); }

  std::chrono::microseconds aging_interval() const noexcept { return aging_; }
  bool aging_enabled() const noexcept { return aging_.count() > 0; }

  // Sort key used when aging is enabled (microseconds; smaller = served first).
  std::int64_t AgedKey(const SchedulingEntry& entry) const noexcept;

  // True when `a` must be served before `b`. A strict total order (sequence is unique).
  bool Before(const SchedulingEntry& a, const SchedulingEntry& b) const noexcept;

  // Priority level plus waiting time in intervals, for display and tests only.
  double EffectiveLevel(const SchedulingEntry& entry, TimePoint now) const noexcept;

 private:
  std::chrono::microseconds aging_;
};

// Indexed binary min-heap of scheduling entries ordered by a PriorityPolicy.
//
//   Push / Pop / Remove   O(log n)
//   Top / Contains / Size O(1)
//   RemoveIf              O(n)
//
// At most one entry per job id: pushing an id that is already queued replaces its
// entry (duplicate protection) and re-sorts it. Remove() invalidates an entry that
// became stale (the job was cancelled, completed, ...). Not thread-safe.
class JobPriorityQueue {
 public:
  explicit JobPriorityQueue(PriorityPolicy policy = PriorityPolicy());

  // Returns true when the job was newly queued, false when an existing entry was replaced.
  bool Push(const SchedulingEntry& entry);

  // The entry that should be served next, without removing it.
  const SchedulingEntry* Top() const noexcept;

  std::optional<SchedulingEntry> Pop();

  // Removes the entry for `id`; returns false when it was not queued.
  bool Remove(const JobId& id);

  // Removes every entry for which `is_stale` returns true; returns how many.
  std::size_t RemoveIf(const std::function<bool(const SchedulingEntry&)>& is_stale);

  bool Contains(const JobId& id) const;
  std::size_t size() const noexcept { return heap_.size(); }
  bool empty() const noexcept { return heap_.empty(); }
  void Clear();

  const PriorityPolicy& policy() const noexcept { return policy_; }

 private:
  void SiftUp(std::size_t index);
  void SiftDown(std::size_t index);
  void Place(std::size_t index, SchedulingEntry entry);
  void RemoveAt(std::size_t index);

  PriorityPolicy policy_;
  std::vector<SchedulingEntry> heap_;
  std::unordered_map<std::string, std::size_t> position_;  // job id -> index in heap_
};

}  // namespace taskflow
