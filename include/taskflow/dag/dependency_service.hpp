#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <vector>

#include "taskflow/dag/dependency_graph.hpp"
#include "taskflow/db/job_repository.hpp"
#include "taskflow/db/postgres.hpp"

namespace taskflow {

// Result of validating a proposed set of dependencies without storing it.
struct GraphCheck {
  bool valid = true;
  std::vector<JobId> cycle;              // closed path (a, b, ..., a) when !valid
  std::vector<JobId> topological_order;  // prerequisites first, when valid
  std::size_t nodes = 0;                 // jobs in the checked graph (existing ancestors + proposed)
  std::size_t edges = 0;
};

// One job of a workflow submitted as a unit. Dependencies are named by `key` (another node of
// the same workflow) or by the id of a job that already exists.
struct WorkflowNode {
  std::string key;  // unique within the workflow, e.g. "fetch"
  NewJob job;       // a node with dependencies must start PENDING (the default)
  std::vector<std::string> depends_on;
  std::vector<JobId> depends_on_existing;
};

struct WorkflowSpec {
  std::vector<WorkflowNode> nodes;
};

struct CreatedWorkflow {
  std::map<std::string, Job> jobs;      // by node key
  std::vector<std::string> order;       // node keys, prerequisites first (the order they were created)
};

struct ResolveResult {
  std::vector<JobId> failed;    // dependents ended FAILED because a prerequisite failed / timed out
  std::vector<JobId> cancelled; // dependents ended CANCELLED because a prerequisite was cancelled
  std::vector<JobId> released;  // PENDING dependents whose prerequisites are now all COMPLETED
};

// Dependency business logic: graph validation, cycle-safe creation, inspection and resolution.
// SQL lives in db/dependency_sql; the graph algorithms in DependencyGraph; this class composes
// them into atomic operations. All methods are thread-safe and safe to call from several
// processes at once (see docs/DATABASE.md, "Concurrency").
//
// Readiness semantics: a job with prerequisites stays PENDING. It becomes eligible for the
// scheduler when every prerequisite is COMPLETED (and its own scheduled_at has passed); the
// scheduler then promotes it to READY exactly once. The database refuses any other path to
// READY. When a prerequisite ends FAILED, TIMEOUT or CANCELLED, its PENDING dependents are
// ended too, transitively, in the same transaction as that transition.
class DependencyService {
 public:
  DependencyService(PgPool& pool, JobRepository& jobs) : pool_(pool), jobs_(jobs) {}

  // Checks a proposed set of edges against the stored graph without storing anything and
  // without locking, so the answer can be stale by the time AddDependencies runs (that call
  // re-checks under a lock). Cycles are reported in the result; malformed input throws
  // ValidationError and unknown jobs throw NotFoundError.
  GraphCheck ValidateEdges(const std::vector<DependencyEdge>& proposed);

  // Adds all edges or none, in one transaction, serialized against every other graph change.
  // Throws CycleError (the cycle path is in the exception), ValidationError (self-dependency,
  // duplicate edge in the request, limits), NotFoundError (unknown job), ConflictError (edge
  // already stored, or the dependent is no longer PENDING). If a prerequisite has already
  // failed or been cancelled, its new dependents are ended immediately (same transaction).
  void AddDependencies(const std::vector<DependencyEdge>& edges);

  // Creates every job and every edge of a workflow in one transaction. Nothing is stored if
  // any part is invalid.
  CreatedWorkflow CreateWorkflow(const WorkflowSpec& workflow);

  // Direct and transitive neighbours.
  std::vector<JobId> Prerequisites(const JobId& job);
  std::vector<JobId> Dependents(const JobId& job);
  std::vector<JobId> Ancestors(const JobId& job);
  std::vector<JobId> Descendants(const JobId& job);
  // The job together with everything it waits for and everything waiting for it, ordered so
  // prerequisites come first.
  std::vector<JobId> TopologicalView(const JobId& job);

  // True when every prerequisite is COMPLETED (vacuously true without prerequisites).
  bool ArePrerequisitesCompleted(const JobId& job);
  std::vector<JobId> UnmetPrerequisites(const JobId& job);

  // Re-evaluates the dependents of `prerequisite` after it changed state. Idempotent. For a
  // failed/timed-out/cancelled prerequisite it ends the PENDING dependents that cannot run
  // (normally already done by the transition itself; this is the repair/inspection path). For a
  // COMPLETED one it reports the dependents that are now eligible.
  ResolveResult ResolveDependents(const JobId& prerequisite);

  // Safety net: finds PENDING jobs that wait for a FAILED/TIMEOUT/CANCELLED prerequisite
  // (for example after a manual database change) and ends them. Returns how many jobs it
  // ended, examining at most `limit` prerequisites.
  std::size_t ResolveBlocked(std::size_t limit);

 private:
  void AddEdgesInTransaction(PgConnection& connection, const std::vector<DependencyEdge>& edges);

  PgPool& pool_;
  JobRepository& jobs_;
};

}  // namespace taskflow
