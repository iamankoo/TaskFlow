#include "taskflow/dag/dependency_service.hpp"

#include <algorithm>
#include <cstdio>
#include <map>
#include <set>
#include <unordered_map>

#include "taskflow/common/errors.hpp"
#include "taskflow/db/dependency_sql.hpp"
#include "taskflow/db/retry.hpp"

namespace taskflow {

namespace {

bool IsFailureOrCancelled(JobState state) {
  return state == JobState::kFailed || state == JobState::kTimeout || state == JobState::kCancelled;
}

// Maps database errors that mean something here onto domain errors. Call from a catch block.
[[noreturn]] void Translate(const DatabaseError& e) {
  const std::string& state = e.sqlstate();
  if (state == "23505") throw ConflictError("dependency already exists (" + e.constraint() + ")");
  if (state == "23503") throw NotFoundError("referenced job does not exist (" + e.constraint() + ")");
  if (state == "23514") throw ValidationError("rejected by constraint " + e.constraint());
  if (state == "TF004") throw ConflictError(e.what());
  throw;
}

template <typename F>
auto InTranslatedRetry(F&& body) -> decltype(body()) {
  try {
    return RetryOnConflict(body);
  } catch (const DatabaseError& e) {
    Translate(e);
  }
}

JobId SyntheticId(std::size_t index) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0000-%012llu", static_cast<unsigned long long>(index + 1));
  return JobId::Parse(buffer);
}

std::string CyclePath(const std::vector<JobId>& cycle) {
  std::string text;
  for (std::size_t i = 0; i < cycle.size(); ++i) text += (i ? " -> " : "") + cycle[i].str();
  return text;
}

[[noreturn]] void ThrowCycle(const std::vector<JobId>& cycle) {
  std::vector<std::string> path;
  for (const auto& id : cycle) path.push_back(id.str());
  throw CycleError("dependency cycle: " + CyclePath(cycle), std::move(path));
}

// Input rules shared by every entry point. Builds the in-memory graph of the proposed edges.
DependencyGraph BuildProposedGraph(const std::vector<DependencyEdge>& edges) {
  if (edges.empty()) throw ValidationError("no dependency edges given");
  if (edges.size() > kMaxEdgesPerRequest) {
    throw ValidationError("too many dependency edges in one request (limit " +
                          std::to_string(kMaxEdgesPerRequest) + ")");
  }
  DependencyGraph graph;
  std::unordered_map<std::string, std::size_t> per_job;
  for (const auto& edge : edges) {
    if (!graph.AddEdge(edge.job, edge.depends_on)) {  // also rejects self-dependencies
      throw ValidationError("duplicate dependency edge: " + edge.job.str() + " -> " + edge.depends_on.str());
    }
    if (++per_job[edge.job.str()] > kMaxPrerequisitesPerJob) {
      throw ValidationError("job " + edge.job.str() + " has more than " +
                            std::to_string(kMaxPrerequisitesPerJob) + " prerequisites");
    }
  }
  return graph;
}

std::vector<JobId> DistinctPrerequisites(const std::vector<DependencyEdge>& edges) {
  std::vector<JobId> out;
  std::set<std::string> seen;
  for (const auto& edge : edges) {
    if (seen.insert(edge.depends_on.str()).second) out.push_back(edge.depends_on);
  }
  return out;
}

std::vector<JobId> AllJobs(const std::vector<DependencyEdge>& edges) {
  std::vector<JobId> out;
  std::set<std::string> seen;
  for (const auto& edge : edges) {
    for (const JobId* id : {&edge.job, &edge.depends_on}) {
      if (seen.insert(id->str()).second) out.push_back(*id);
    }
  }
  return out;
}

}  // namespace

// ---------- validation ----------

GraphCheck DependencyService::ValidateEdges(const std::vector<DependencyEdge>& proposed) {
  DependencyGraph graph = BuildProposedGraph(proposed);
  auto lease = pool_.Acquire();
  PgConnection& connection = *lease;

  // Unknown jobs are an error, not a "valid" answer.
  const auto jobs = AllJobs(proposed);
  std::string array = "{";
  for (std::size_t i = 0; i < jobs.size(); ++i) array += (i ? "," : "") + jobs[i].str();
  array += "}";
  const auto found = connection.Query("SELECT count(*) FROM jobs WHERE id = ANY($1::uuid[])", {array});
  if (std::stoull(found.Value(0, 0)) != jobs.size()) throw NotFoundError("one or more jobs do not exist");

  const auto existing = dependency_sql::LoadAncestorEdges(connection, DistinctPrerequisites(proposed),
                                                          kMaxGraphClosureEdges);
  for (const auto& edge : existing) graph.AddEdge(edge.job, edge.depends_on);

  GraphCheck check;
  check.nodes = graph.node_count();
  check.edges = graph.edge_count();
  if (const auto cycle = graph.FindCycle()) {
    check.valid = false;
    check.cycle = *cycle;
    return check;
  }
  check.topological_order = graph.TopologicalOrder();
  return check;
}

// ---------- creation ----------

void DependencyService::AddEdgesInTransaction(PgConnection& connection,
                                              const std::vector<DependencyEdge>& edges) {
  DependencyGraph graph = BuildProposedGraph(edges);

  // One graph writer at a time, across all processes. Two transactions that each add one half of
  // a cycle cannot both pass the check below, because the second one waits here until the
  // first has committed and then sees its edges.
  dependency_sql::TakeGraphLock(connection);

  const auto locked = dependency_sql::LockJobs(connection, AllJobs(edges));
  std::map<std::string, JobState> state;
  for (const auto& job : locked) state[job.id.str()] = job.state;

  std::set<std::string> dependents;
  for (const auto& edge : edges) dependents.insert(edge.job.str());
  for (const auto& id : dependents) {
    if (state.at(id) != JobState::kPending) {
      throw ConflictError("job " + id + " is " + std::string(ToString(state.at(id))) +
                          "; prerequisites can only be added to a PENDING job");
    }
  }

  // A new edge "J waits for P" closes a cycle exactly when P already (transitively) waits for J,
  // so the stored ancestors of every P, plus the new edges, contain everything that matters.
  const auto ancestors =
      dependency_sql::LoadAncestorEdges(connection, DistinctPrerequisites(edges), kMaxGraphClosureEdges);
  for (const auto& edge : ancestors) graph.AddEdge(edge.job, edge.depends_on);  // duplicates are fine here
  if (const auto cycle = graph.FindCycle()) ThrowCycle(*cycle);

  dependency_sql::InsertEdges(connection, edges);

  // A prerequisite that has already failed will never complete: its new dependents end now.
  for (const JobId& prerequisite : DistinctPrerequisites(edges)) {
    const JobState prerequisite_state = state.at(prerequisite.str());
    if (IsFailureOrCancelled(prerequisite_state)) {
      dependency_sql::PropagateFailure(connection, prerequisite, prerequisite_state);
    }
  }
}

void DependencyService::AddDependencies(const std::vector<DependencyEdge>& edges) {
  BuildProposedGraph(edges);  // cheap, lock-free rejection of malformed requests
  InTranslatedRetry([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    AddEdgesInTransaction(tx.connection(), edges);
    tx.Commit();
  });
}

CreatedWorkflow DependencyService::CreateWorkflow(const WorkflowSpec& workflow) {
  const auto& nodes = workflow.nodes;
  if (nodes.empty()) throw ValidationError("a workflow needs at least one job");
  if (nodes.size() > kMaxWorkflowJobs) {
    throw ValidationError("a workflow may contain at most " + std::to_string(kMaxWorkflowJobs) + " jobs");
  }

  // Validate shape and dependencies in memory, using stand-in ids, before touching the database.
  std::unordered_map<std::string, std::size_t> position;
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    if (nodes[i].key.empty()) throw ValidationError("workflow node " + std::to_string(i) + " has no key");
    if (!position.emplace(nodes[i].key, i).second) {
      throw ValidationError("duplicate workflow key '" + nodes[i].key + "'");
    }
  }
  DependencyGraph shape;
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    shape.AddNode(SyntheticId(i));
    const bool has_dependencies = !nodes[i].depends_on.empty() || !nodes[i].depends_on_existing.empty();
    if (has_dependencies && nodes[i].job.initial_state != JobState::kPending) {
      throw ValidationError("workflow node '" + nodes[i].key + "' has prerequisites, so it must start PENDING");
    }
    if (nodes[i].depends_on.size() + nodes[i].depends_on_existing.size() > kMaxPrerequisitesPerJob) {
      throw ValidationError("workflow node '" + nodes[i].key + "' has too many prerequisites");
    }
    for (const auto& key : nodes[i].depends_on) {
      const auto found = position.find(key);
      if (found == position.end()) {
        throw ValidationError("workflow node '" + nodes[i].key + "' depends on unknown key '" + key + "'");
      }
      if (!shape.AddEdge(SyntheticId(i), SyntheticId(found->second))) {
        throw ValidationError("workflow node '" + nodes[i].key + "' lists '" + key + "' twice");
      }
    }
  }
  const auto node_of = [](const JobId& stand_in) { return std::stoull(stand_in.str().substr(24)) - 1; };
  std::vector<JobId> stand_in_order;
  try {
    stand_in_order = shape.TopologicalOrder();
  } catch (const CycleError& e) {  // report the cycle with the caller's keys, not stand-in ids
    std::vector<std::string> keys;
    std::string text;
    for (const auto& id : e.cycle()) {
      const std::string& key = nodes[node_of(JobId::Parse(id))].key;
      text += (keys.empty() ? "" : " -> ") + key;
      keys.push_back(key);
    }
    throw CycleError("workflow dependency cycle: " + text, std::move(keys));
  }
  std::vector<std::size_t> creation_order;
  for (const auto& id : stand_in_order) creation_order.push_back(node_of(id));

  return InTranslatedRetry([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    CreatedWorkflow created;
    std::vector<DependencyEdge> edges;
    for (const std::size_t index : creation_order) {
      const WorkflowNode& node = nodes[index];
      Job job = jobs_.CreateJobInTransaction(tx.connection(), node.job);
      for (const auto& key : node.depends_on) edges.push_back({job.id, created.jobs.at(key).id});
      for (const JobId& existing : node.depends_on_existing) edges.push_back({job.id, existing});
      created.order.push_back(node.key);
      created.jobs.emplace(node.key, std::move(job));
    }
    if (!edges.empty()) AddEdgesInTransaction(tx.connection(), edges);
    tx.Commit();
    return created;
  });
}

// ---------- inspection ----------

std::vector<JobId> DependencyService::Prerequisites(const JobId& job) {
  auto lease = pool_.Acquire();
  return dependency_sql::Prerequisites(*lease, job);
}

std::vector<JobId> DependencyService::Dependents(const JobId& job) {
  auto lease = pool_.Acquire();
  return dependency_sql::Dependents(*lease, job);
}

std::vector<JobId> DependencyService::Ancestors(const JobId& job) {
  auto lease = pool_.Acquire();
  DependencyGraph graph;
  graph.AddNode(job);
  for (const auto& edge : dependency_sql::LoadAncestorEdges(*lease, {job}, kMaxGraphClosureEdges)) {
    graph.AddEdge(edge.job, edge.depends_on);
  }
  return graph.Ancestors(job);
}

std::vector<JobId> DependencyService::Descendants(const JobId& job) {
  auto lease = pool_.Acquire();
  DependencyGraph graph;
  graph.AddNode(job);
  for (const auto& edge : dependency_sql::LoadDescendantEdges(*lease, {job}, kMaxGraphClosureEdges)) {
    graph.AddEdge(edge.job, edge.depends_on);
  }
  return graph.Descendants(job);
}

std::vector<JobId> DependencyService::TopologicalView(const JobId& job) {
  auto lease = pool_.Acquire();
  DependencyGraph graph;
  graph.AddNode(job);
  for (const auto& edge : dependency_sql::LoadAncestorEdges(*lease, {job}, kMaxGraphClosureEdges)) {
    graph.AddEdge(edge.job, edge.depends_on);
  }
  for (const auto& edge : dependency_sql::LoadDescendantEdges(*lease, {job}, kMaxGraphClosureEdges)) {
    graph.AddEdge(edge.job, edge.depends_on);
  }
  return graph.TopologicalOrder();
}

std::vector<JobId> DependencyService::UnmetPrerequisites(const JobId& job) {
  auto lease = pool_.Acquire();
  const auto exists = lease->Query("SELECT count(*) FROM jobs WHERE id = $1::uuid", {job.str()});
  if (exists.Value(0, 0) != "1") throw NotFoundError("job " + job.str() + " does not exist");
  return dependency_sql::UnmetPrerequisites(*lease, job);
}

bool DependencyService::ArePrerequisitesCompleted(const JobId& job) { return UnmetPrerequisites(job).empty(); }

// ---------- resolution ----------

ResolveResult DependencyService::ResolveDependents(const JobId& prerequisite) {
  return InTranslatedRetry([&] {
    auto lease = pool_.Acquire();
    PgTransaction tx(*lease);
    ResolveResult result;
    const auto locked = dependency_sql::LockJobs(tx.connection(), {prerequisite});
    const JobState state = locked.front().state;
    if (IsFailureOrCancelled(state)) {
      for (const auto& ended : dependency_sql::PropagateFailure(tx.connection(), prerequisite, state)) {
        (ended.state == JobState::kCancelled ? result.cancelled : result.failed).push_back(ended.job);
      }
    } else if (state == JobState::kCompleted) {
      result.released = dependency_sql::ReleasableDependents(tx.connection(), prerequisite);
    }
    tx.Commit();
    return result;
  });
}

std::size_t DependencyService::ResolveBlocked(std::size_t limit) {
  std::vector<std::pair<JobId, JobState>> blockers;
  {
    auto lease = pool_.Acquire();
    const auto rows = lease->Query(
        "SELECT DISTINCT p.id::text, p.status FROM job_dependencies d "
        "JOIN jobs j ON j.id = d.job_id AND j.status = 'PENDING' "
        "JOIN jobs p ON p.id = d.depends_on_id AND p.status IN ('FAILED', 'TIMEOUT', 'CANCELLED') "
        "LIMIT $1::bigint",
        {std::to_string(limit)});
    for (std::size_t i = 0; i < rows.rows.size(); ++i) {
      blockers.emplace_back(JobId::Parse(rows.Value(i, 0)), ParseJobState(rows.Value(i, 1)));
    }
  }
  std::size_t ended = 0;
  for (const auto& [id, state] : blockers) {
    const ResolveResult result = ResolveDependents(id);
    ended += result.failed.size() + result.cancelled.size();
    (void)state;
  }
  return ended;
}

}  // namespace taskflow
