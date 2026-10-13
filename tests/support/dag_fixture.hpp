#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "scheduler_fixture.hpp"
#include "taskflow/dag/dependency_service.hpp"

namespace taskflow::testing {

// SchedulerFixture plus a DependencyService and helpers to drive jobs through their lifecycle
// without a worker (Phase 5 does not exist yet).
class DagFixture : public SchedulerFixture {
 protected:
  void SetUp() override {
    SchedulerFixture::SetUp();
    deps_ = std::make_unique<DependencyService>(*pool_, *repo_);
  }

  // A PENDING job that is already due (scheduled in the past).
  SchedulingEntry Due(JobPriority priority = JobPriority::kNormal) { return Pending(priority, Ms(-1000)); }

  void Edge(const SchedulingEntry& job, const SchedulingEntry& prerequisite) {
    deps_->AddDependencies({{job.id, prerequisite.id}});
  }

  // Moves a job to READY (if it is PENDING and unblocked) then RUNNING, returning the attempt.
  JobAttempt Start(const SchedulingEntry& job) {
    if (State(job) == JobState::kPending) repo_->UpdateJobState(job.id, JobState::kReady);
    return repo_->CreateAttempt(job.id);
  }

  // Runs a job to the given final state the way a worker would (success -> COMPLETED, ...).
  void Finish(const SchedulingEntry& job, JobState final_state) {
    if (final_state == JobState::kCancelled && State(job) != JobState::kRunning) {
      repo_->UpdateJobState(job.id, JobState::kCancelled);
      return;
    }
    const JobAttempt attempt = Start(job);
    AttemptOutcome outcome;
    switch (final_state) {
      case JobState::kCompleted: outcome.state = AttemptState::kSucceeded; break;
      case JobState::kFailed: outcome.state = AttemptState::kFailed; outcome.error_message = "boom"; break;
      case JobState::kTimeout: outcome.state = AttemptState::kTimedOut; outcome.error_message = "too slow"; break;
      case JobState::kCancelled: outcome.state = AttemptState::kCancelled; outcome.error_message = "stop"; break;
      default: throw std::invalid_argument("Finish needs a final state");
    }
    repo_->StoreResult(job.id, attempt.attempt_number, outcome, final_state);
  }
  void Complete(const SchedulingEntry& job) { Finish(job, JobState::kCompleted); }

  JobState State(const SchedulingEntry& job) { return repo_->GetJob(job.id)->state; }
  std::optional<std::string> LastError(const SchedulingEntry& job) { return repo_->GetJob(job.id)->last_error; }

  // Every READY event id for a job (there should be at most one per release).
  long ReadyEvents(const SchedulingEntry& job) {
    return Count("job_events", "to_status = 'READY' AND job_id = '" + job.id.str() + "'");
  }

  std::vector<DependencyEdge> AllEdges() {
    std::vector<DependencyEdge> edges;
    const auto rows = Sql("SELECT job_id::text, depends_on_id::text FROM job_dependencies");
    for (std::size_t i = 0; i < rows.rows.size(); ++i) {
      edges.push_back({JobId::Parse(rows.Value(i, 0)), JobId::Parse(rows.Value(i, 1))});
    }
    return edges;
  }

  std::unique_ptr<DependencyService> deps_;
};

}  // namespace taskflow::testing
