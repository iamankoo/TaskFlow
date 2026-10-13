#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "taskflow/config/config.hpp"
#include "taskflow/dag/dependency_service.hpp"
#include "taskflow/db/job_repository.hpp"
#include "taskflow/scheduler/priority_queue.hpp"
#include "taskflow/scheduler/schedule_queue.hpp"

namespace taskflow {

// What one PostgreSQL -> Redis repair pass changed.
struct ReconcileReport {
  bool ran = false;            // false when another scheduler held the reconcile lock
  std::size_t ready_added = 0;     // READY in PostgreSQL but missing (or wrongly scored) in Redis
  std::size_t ready_removed = 0;   // in a Redis ready queue but no longer READY (or other priority)
  std::size_t delayed_added = 0;
  std::size_t delayed_removed = 0;

  std::size_t repairs() const { return ready_added + ready_removed + delayed_added + delayed_removed; }
};

struct TickResult {
  std::size_t candidates = 0;  // due jobs read from PostgreSQL
  std::size_t promoted = 0;    // jobs this tick moved to READY
  bool redis_ok = true;        // false when publishing/reconciling hit a Redis error
  std::size_t dependents_ended = 0;  // jobs ended by the dependency safety net this tick
  ReconcileReport reconcile;
  std::chrono::microseconds duration{0};
  std::vector<SchedulingEntry> promoted_entries;  // in the order they were chosen
};

// Decides which persisted jobs become runnable next and makes them available to
// future workers.
//
// One Tick():
//   1. read due PENDING/RETRY_WAIT jobs from PostgreSQL (idx_jobs_due)
//   2. order them with the priority heap (priority, FIFO, aging) and take the best N
//   3. promote those N to READY in one PostgreSQL transaction (row locks, SKIP LOCKED)
//   4. publish the promoted jobs to the Redis ready queues
//   5. periodically (or when Redis looks empty/lost) reconcile Redis from PostgreSQL
//
// PostgreSQL is always authoritative: a Redis failure never blocks or undoes a
// promotion, and reconciliation rebuilds Redis from PostgreSQL. Any number of
// Scheduler instances may run against the same database: promotion uses
// FOR UPDATE SKIP LOCKED, Redis writes are idempotent, and only reconciliation
// takes a short-lived Redis lock. The scheduler never touches job state beyond
// PENDING/RETRY_WAIT -> READY. Tick() and Reconcile() are safe to call concurrently.
class Scheduler {
 public:
  using Clock = std::function<TimePoint()>;

  Scheduler(JobRepository& jobs, ScheduleQueue& queue, SchedulerConfig config,
            Clock clock = [] { return std::chrono::system_clock::now(); },
            std::string instance_id = "");
  ~Scheduler();

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  // One synchronous scheduling cycle. Throws DatabaseError if PostgreSQL fails; Redis
  // failures are reported in the result instead.
  TickResult Tick();

  // Repairs Redis from PostgreSQL. Throws RedisError/DatabaseError on failure.
  ReconcileReport Reconcile();

  // Optional. With a dependency service, the periodic pass also ends PENDING jobs that wait for a
  // prerequisite that failed or was cancelled (a safety net: normally the transition that
  // ended the prerequisite already did it). Promotion itself never needs it: jobs with unmet
  // prerequisites are simply not promotable (see JobRepository::PromoteJobsToReady).
  void SetDependencyService(DependencyService* dependencies) { dependencies_ = dependencies; }

  // Publish-after-commit hook for code that just persisted jobs: READY jobs go to the
  // ready queues, PENDING/RETRY_WAIT jobs to the delayed set. Other states are ignored.
  void Publish(const std::vector<SchedulingEntry>& entries);

  // Background loop: Tick(), then wait tick_interval_ms (or until Stop). Errors are logged
  // and the loop continues. Start is idempotent; Stop blocks until the loop has exited.
  void Start();
  void Stop();
  bool running() const { return running_.load(); }

  std::uint64_t ticks() const { return ticks_.load(); }
  const std::string& instance_id() const { return instance_id_; }
  const PriorityPolicy& policy() const { return policy_; }

 private:
  void Loop();

  JobRepository& jobs_;
  ScheduleQueue& queue_;
  SchedulerConfig config_;
  Clock clock_;
  std::string instance_id_;
  PriorityPolicy policy_;

  DependencyService* dependencies_ = nullptr;
  std::atomic<std::uint64_t> ticks_{0};
  std::atomic<bool> need_reconcile_{false};

  std::mutex control_mutex_;  // serializes Start/Stop
  std::mutex wait_mutex_;     // pairs with wake_ so a Stop() can never be missed
  std::condition_variable wake_;
  std::thread thread_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
};

}  // namespace taskflow
