#pragma once

#include <memory>
#include <string>
#include <vector>

#include "schema_fixture.hpp"
#include "taskflow/common/errors.hpp"
#include "taskflow/db/job_repository.hpp"
#include "taskflow/redis/redis_client.hpp"
#include "taskflow/scheduler/schedule_queue.hpp"
#include "taskflow/scheduler/scheduler.hpp"

namespace taskflow::testing {

// A migrated throwaway PostgreSQL schema plus an isolated Redis database (number 15,
// flushed before and after every test, so it is safe to FLUSHDB for real).
//
// Time is deterministic: `base_` is a whole second one hour in the past and `now_` is the
// clock handed to every Scheduler. Jobs are created relative to `base_`.
class SchedulerFixture : public SchemaFixture {
 protected:
  using Ms = std::chrono::milliseconds;

  void SetUp() override {
    SchemaFixture::SetUp();
    config_.redis.database = 15;
    redis_ = std::make_unique<RedisClient>(config_.redis);
    redis_->Execute({"FLUSHDB"});
    prefix_ = UniqueName("tfq");
    queue_ = std::make_unique<ScheduleQueue>(*redis_, prefix_);
    repo_ = std::make_unique<JobRepository>(*pool_);

    const auto hour_ago = std::chrono::system_clock::now() - std::chrono::hours(1);
    base_ = std::chrono::time_point_cast<std::chrono::seconds>(hour_ago);
    now_ = base_;

    scheduler_config_.tick_interval_ms = 20;
    scheduler_config_.aging_interval_ms = 0;  // strict priority unless a test enables aging
    scheduler_config_.redis_prefix = prefix_;
  }

  void TearDown() override {
    try {
      redis_->Execute({"FLUSHDB"});
    } catch (const Error&) {
    }
    SchemaFixture::TearDown();
  }

  std::unique_ptr<Scheduler> MakeScheduler(const std::string& id = "test-scheduler") {
    return std::make_unique<Scheduler>(*repo_, *queue_, scheduler_config_, [this] { return now_; }, id);
  }

  // Creates a job whose scheduled_at is base_ + offset and returns its scheduling data.
  SchedulingEntry MakeJob(JobPriority priority, JobState state, Ms offset = Ms(0),
                          const std::string& type = "test.job") {
    NewJob job;
    job.type = type;
    job.priority = priority;
    job.initial_state = state;
    job.scheduled_at = base_ + offset;
    const Job created = repo_->CreateJob(job);
    return repo_->GetSchedulingEntries({created.id}).front();
  }
  SchedulingEntry Pending(JobPriority p, Ms offset = Ms(0)) { return MakeJob(p, JobState::kPending, offset); }
  SchedulingEntry Ready(JobPriority p, Ms offset = Ms(0)) { return MakeJob(p, JobState::kReady, offset); }

  JobState StateOf(const SchedulingEntry& entry) { return repo_->GetJob(entry.id)->state; }

  // Sequences of jobs in the order the heap/Redis would serve them.
  std::vector<std::int64_t> Sequences(const std::vector<SchedulingEntry>& entries) {
    std::vector<std::int64_t> out;
    for (const auto& entry : entries) out.push_back(entry.sequence);
    return out;
  }

  std::size_t TotalReady() {
    std::size_t total = 0;
    for (const JobPriority p : kPrioritiesHighestFirst) total += queue_->ReadyCount(p);
    return total;
  }

  std::unique_ptr<RedisClient> redis_;
  std::unique_ptr<ScheduleQueue> queue_;
  std::unique_ptr<JobRepository> repo_;
  std::string prefix_;
  SchedulerConfig scheduler_config_;
  TimePoint base_;
  TimePoint now_;
};

}  // namespace taskflow::testing
