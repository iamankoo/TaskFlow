#pragma once

#include <cstddef>
#include <vector>

#include "taskflow/dag/dependency_graph.hpp"
#include "taskflow/db/postgres.hpp"
#include "taskflow/jobs/job.hpp"

// Transaction-scoped SQL for the dependency graph. Every function runs on a connection the
// caller already holds inside a transaction and never commits, so callers compose them into
// one atomic unit (JobRepository for state changes, DependencyService for graph changes).
namespace taskflow::dependency_sql {

struct LockedJob {
  JobId id;
  JobState state;
};

// Serializes every transaction that adds dependency edges, across all processes: a
// transaction-scoped PostgreSQL advisory lock, released automatically at COMMIT/ROLLBACK
// (so a crashed client can never leave it held).
void TakeGraphLock(PgConnection& connection);

// Locks the given jobs FOR UPDATE in ascending id order and returns their states.
// Throws NotFoundError naming the missing ids.
std::vector<LockedJob> LockJobs(PgConnection& connection, std::vector<JobId> ids);

// Every edge reachable by following "waits for" from `roots` (their transitive
// prerequisites), de-duplicated. Throws ValidationError when more than `max_edges` exist.
std::vector<DependencyEdge> LoadAncestorEdges(PgConnection& connection, const std::vector<JobId>& roots,
                                              std::size_t max_edges);

// Same walk in the other direction: edges among the transitive dependents of `roots`.
std::vector<DependencyEdge> LoadDescendantEdges(PgConnection& connection, const std::vector<JobId>& roots,
                                                std::size_t max_edges);

// Inserts edges in one statement. A duplicate edge raises DatabaseError 23505.
void InsertEdges(PgConnection& connection, const std::vector<DependencyEdge>& edges);

std::vector<JobId> Prerequisites(PgConnection& connection, const JobId& job);
std::vector<JobId> Dependents(PgConnection& connection, const JobId& job);
// Prerequisites that are not COMPLETED.
std::vector<JobId> UnmetPrerequisites(PgConnection& connection, const JobId& job);

struct Propagated {
  JobId job;
  JobState state;     // FAILED or CANCELLED
  JobId caused_by;    // the prerequisite whose outcome decided it
};

// A job ended FAILED, TIMEOUT or CANCELLED: every PENDING job that (transitively) waits for it can
// never run, so it is ended too, in the same transaction, one job_events row each.
//   prerequisite FAILED or TIMEOUT  ->  dependent FAILED     (PENDING -> FAILED)
//   prerequisite CANCELLED          ->  dependent CANCELLED  (PENDING -> CANCELLED)
// A dependent with several non-successful prerequisites ends FAILED if any of them failed.
// Dependents that are not PENDING are left alone (they cannot be waiting on this job).
// Idempotent. Returns the jobs it ended, in the order they were ended.
std::vector<Propagated> PropagateFailure(PgConnection& connection, const JobId& origin, JobState origin_state);

// Dependents of a prerequisite that are PENDING and whose prerequisites are all COMPLETED.
std::vector<JobId> ReleasableDependents(PgConnection& connection, const JobId& prerequisite);

}  // namespace taskflow::dependency_sql
