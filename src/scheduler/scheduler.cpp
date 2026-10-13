#include "taskflow/scheduler/scheduler.hpp"

#include <spdlog/spdlog.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <map>
#include <random>
#include <sstream>
#include <unordered_map>

#include "taskflow/common/errors.hpp"

namespace taskflow {

namespace {

constexpr const char* kReconcileLock = "reconcile";

std::string MakeInstanceId() {
  std::random_device device;
  std::ostringstream out;
  out << "scheduler-" << ::getpid() << "-" << std::hex << device() << device();
  return out.str();
}

std::size_t QueueIndex(JobPriority priority) {
  for (std::size_t i = 0; i < kPrioritiesHighestFirst.size(); ++i) {
    if (kPrioritiesHighestFirst[i] == priority) return i;
  }
  return 0;
}

using MemberMap = std::map<std::string, std::int64_t>;  // member -> score (microseconds)

// Every member of one Redis sorted set, read page by page.
template <typename ReadPage>
MemberMap ReadAll(std::size_t page_size, ReadPage&& read_page) {
  MemberMap all;
  for (std::size_t offset = 0;; offset += page_size) {
    const auto page = read_page(offset, page_size);
    for (const auto& [member, score] : page) all[member] = score;
    if (page.size() < page_size) break;
  }
  return all;
}

}  // namespace

Scheduler::Scheduler(JobRepository& jobs, ScheduleQueue& queue, SchedulerConfig config, Clock clock,
                     std::string instance_id)
    : jobs_(jobs),
      queue_(queue),
      config_(std::move(config)),
      clock_(std::move(clock)),
      instance_id_(instance_id.empty() ? MakeInstanceId() : std::move(instance_id)),
      policy_(std::chrono::microseconds(static_cast<std::int64_t>(config_.aging_interval_ms) * 1000)) {}

Scheduler::~Scheduler() { Stop(); }

TickResult Scheduler::Tick() {
  const auto started = std::chrono::steady_clock::now();
  const std::uint64_t tick_number = ticks_.fetch_add(1);
  const TimePoint now = clock_();
  TickResult result;

  // 1. Candidates: PostgreSQL finds what is due; it does not decide the order.
  const auto due = jobs_.FindDueJobs(now, config_.candidates_per_priority);
  result.candidates = due.size();

  // 2. The scheduling decision: heap order (priority, FIFO, aging), best N first.
  JobPriorityQueue heap(policy_);
  for (const auto& entry : due) heap.Push(entry);
  std::vector<JobId> chosen_ids;
  std::vector<SchedulingEntry> chosen;
  while (chosen.size() < config_.max_promotions_per_tick) {
    auto next = heap.Pop();
    if (!next) break;
    chosen_ids.push_back(next->id);
    chosen.push_back(std::move(*next));
  }

  // 3. Promote in one transaction. Rows another scheduler already took are skipped.
  const auto promoted = jobs_.PromoteJobsToReady(chosen_ids, now);
  result.promoted = promoted.size();
  result.promoted_entries = promoted;

  const bool periodic = tick_number % config_.reconcile_interval_ticks == 0;
  if (periodic && dependencies_ != nullptr) {
    result.dependents_ended = dependencies_->ResolveBlocked(config_.max_promotions_per_tick);
  }

  // 4./5. Make them available, then repair Redis if it is due. PostgreSQL has already
  // committed, so a Redis failure only schedules a repair for later.
  try {
    if (!promoted.empty()) queue_.MarkReady(promoted);
    if (periodic || need_reconcile_.load() || !queue_.HasSentinel()) {
      result.reconcile = Reconcile();
      if (result.reconcile.ran) need_reconcile_.store(false);
    }
  } catch (const RedisError& e) {
    result.redis_ok = false;
    need_reconcile_.store(true);
    spdlog::warn("scheduler {}: Redis unavailable, will reconcile later: {}", instance_id_, e.what());
  }

  result.duration = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - started);
  return result;
}

void Scheduler::Publish(const std::vector<SchedulingEntry>& entries) {
  std::vector<SchedulingEntry> ready;
  std::vector<SchedulingEntry> delayed;
  for (const auto& entry : entries) {
    if (entry.state == JobState::kReady) {
      ready.push_back(entry);
    } else if (entry.state == JobState::kPending || entry.state == JobState::kRetryWait) {
      delayed.push_back(entry);
    }
  }
  try {
    if (!ready.empty()) queue_.AddReady(ready);
    if (!delayed.empty()) queue_.AddDelayed(delayed);
  } catch (const RedisError&) {
    need_reconcile_.store(true);  // PostgreSQL already has the jobs; reconciliation catches up
    throw;
  }
}

ReconcileReport Scheduler::Reconcile() {
  ReconcileReport report;
  const auto ttl = std::chrono::milliseconds(config_.lock_ttl_ms);
  if (!queue_.TryLock(kReconcileLock, instance_id_, ttl)) return report;  // someone else is repairing
  struct Unlock {
    ScheduleQueue& queue;
    const std::string& owner;
    ~Unlock() {
      try {
        queue.Unlock(kReconcileLock, owner);
      } catch (const Error&) {
        // the lock expires by itself
      }
    }
  } unlock{queue_, instance_id_};
  report.ran = true;
  const std::size_t page = config_.scan_batch_size;

  // What PostgreSQL says should be in Redis.
  std::array<std::map<std::string, SchedulingEntry>, 4> expected_ready;
  for (const JobPriority priority : kPrioritiesHighestFirst) {
    std::optional<ScanCursor> cursor;
    for (;;) {
      const auto rows = jobs_.FindReadyJobs(priority, cursor, page);
      for (const auto& entry : rows) {
        expected_ready[QueueIndex(priority)].emplace(ScheduleQueue::MakeMember(entry), entry);
      }
      if (rows.size() < page) break;
      cursor = ScanCursor{rows.back().scheduled_at, rows.back().sequence};
    }
  }
  std::map<std::string, SchedulingEntry> expected_delayed;
  {
    std::optional<ScanCursor> cursor;
    for (;;) {
      const auto rows = jobs_.FindWaitingJobs(cursor, page);
      for (const auto& entry : rows) expected_delayed.emplace(ScheduleQueue::MakeMember(entry), entry);
      if (rows.size() < page) break;
      cursor = ScanCursor{rows.back().scheduled_at, rows.back().sequence};
    }
  }

  // What Redis actually holds.
  std::array<MemberMap, 4> actual_ready;
  for (const JobPriority priority : kPrioritiesHighestFirst) {
    actual_ready[QueueIndex(priority)] = ReadAll(page, [&](std::size_t offset, std::size_t count) {
      return queue_.ReadyMembers(priority, offset, count);
    });
  }
  const MemberMap actual_delayed = ReadAll(page, [&](std::size_t offset, std::size_t count) {
    return queue_.DelayedMembers(offset, count);
  });

  // Missing or wrongly scored in Redis -> add.
  std::vector<SchedulingEntry> add_ready;
  std::vector<SchedulingEntry> add_delayed;
  for (const JobPriority priority : kPrioritiesHighestFirst) {
    const auto& have = actual_ready[QueueIndex(priority)];
    for (const auto& [member, entry] : expected_ready[QueueIndex(priority)]) {
      const auto found = have.find(member);
      if (found == have.end() || found->second != ScheduleQueue::ScoreOf(entry)) add_ready.push_back(entry);
    }
  }
  for (const auto& [member, entry] : expected_delayed) {
    const auto found = actual_delayed.find(member);
    if (found == actual_delayed.end() || found->second != ScheduleQueue::ScoreOf(entry)) {
      add_delayed.push_back(entry);
    }
  }

  // In Redis but not expected -> candidate for removal, confirmed against PostgreSQL first
  // (a job another scheduler promoted a moment ago is expected on the next pass, not stale).
  std::array<std::vector<std::string>, 4> stale_ready;
  std::vector<std::string> stale_delayed;
  std::vector<JobId> to_verify;
  const auto consider = [&](const std::string& member) {
    if (const auto parsed = ScheduleQueue::ParseMember(member)) to_verify.push_back(parsed->second);
  };
  for (const JobPriority priority : kPrioritiesHighestFirst) {
    for (const auto& [member, score] : actual_ready[QueueIndex(priority)]) {
      if (!expected_ready[QueueIndex(priority)].count(member)) {
        stale_ready[QueueIndex(priority)].push_back(member);
        consider(member);
      }
    }
  }
  for (const auto& [member, score] : actual_delayed) {
    if (!expected_delayed.count(member)) {
      stale_delayed.push_back(member);
      consider(member);
    }
  }
  std::unordered_map<std::string, SchedulingEntry> truth;
  for (std::size_t begin = 0; begin < to_verify.size(); begin += page) {
    const std::size_t end = std::min(to_verify.size(), begin + page);
    const std::vector<JobId> chunk(to_verify.begin() + static_cast<std::ptrdiff_t>(begin),
                                   to_verify.begin() + static_cast<std::ptrdiff_t>(end));
    for (const auto& entry : jobs_.GetSchedulingEntries(chunk)) truth.emplace(entry.id.str(), entry);
  }
  const auto still_valid = [&](const std::string& member, const std::function<bool(const SchedulingEntry&)>& ok) {
    const auto parsed = ScheduleQueue::ParseMember(member);
    if (!parsed) return false;  // malformed: never valid
    const auto found = truth.find(parsed->second.str());
    return found != truth.end() && parsed->first == found->second.sequence && ok(found->second);
  };

  for (const JobPriority priority : kPrioritiesHighestFirst) {
    std::vector<std::string> remove;
    for (const auto& member : stale_ready[QueueIndex(priority)]) {
      const bool valid = still_valid(member, [&](const SchedulingEntry& e) {
        return e.state == JobState::kReady && e.priority == priority;
      });
      if (!valid) remove.push_back(member);
    }
    if (!remove.empty()) {
      queue_.RemoveReady(priority, remove);
      report.ready_removed += remove.size();
    }
  }
  std::vector<std::string> remove_delayed;
  for (const auto& member : stale_delayed) {
    const bool valid = still_valid(member, [](const SchedulingEntry& e) {
      return e.state == JobState::kPending || e.state == JobState::kRetryWait;
    });
    if (!valid) remove_delayed.push_back(member);
  }
  if (!remove_delayed.empty()) {
    queue_.RemoveDelayed(remove_delayed);
    report.delayed_removed += remove_delayed.size();
  }

  if (!add_ready.empty()) queue_.AddReady(add_ready);
  if (!add_delayed.empty()) queue_.AddDelayed(add_delayed);
  report.ready_added = add_ready.size();
  report.delayed_added = add_delayed.size();

  queue_.SetSentinel();
  if (report.repairs() > 0) {
    spdlog::info("scheduler {}: reconciled Redis (+{} ready, -{} ready, +{} delayed, -{} delayed)",
                 instance_id_, report.ready_added, report.ready_removed, report.delayed_added,
                 report.delayed_removed);
  }
  return report;
}

void Scheduler::Start() {
  std::lock_guard lock(control_mutex_);
  if (thread_.joinable()) return;
  stop_requested_.store(false);
  running_.store(true);
  thread_ = std::thread([this] { Loop(); });
}

void Scheduler::Stop() {
  std::lock_guard lock(control_mutex_);  // serializes concurrent Stop() calls
  if (!thread_.joinable()) return;
  {
    std::lock_guard<std::mutex> flag_lock(wait_mutex_);
    stop_requested_.store(true);
  }
  wake_.notify_all();
  thread_.join();
  running_.store(false);
}

void Scheduler::Loop() {
  spdlog::info("scheduler {} started (tick {} ms, aging {} ms)", instance_id_, config_.tick_interval_ms,
               config_.aging_interval_ms);
  while (!stop_requested_.load()) {
    try {
      const TickResult result = Tick();
      if (result.promoted > 0) {
        spdlog::info("scheduler {}: promoted {} job(s) in {} us", instance_id_, result.promoted,
                     result.duration.count());
      }
    } catch (const std::exception& e) {
      spdlog::error("scheduler {}: tick failed: {}", instance_id_, e.what());
    }
    std::unique_lock lock(wait_mutex_);
    wake_.wait_for(lock, std::chrono::milliseconds(config_.tick_interval_ms),
                   [this] { return stop_requested_.load(); });
  }
  spdlog::info("scheduler {} stopped", instance_id_);
}

}  // namespace taskflow
