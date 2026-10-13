#include "taskflow/db/dependency_sql.hpp"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "taskflow/common/errors.hpp"

namespace taskflow::dependency_sql {

namespace {

// Arbitrary constant naming "TaskFlow dependency-graph mutation" for pg_advisory_xact_lock.
// (The migration runner uses 727274; this one must differ.)
constexpr const char* kGraphLockKey = "727275";

std::string UuidArray(const std::vector<JobId>& ids) {
  std::string out = "{";
  for (std::size_t i = 0; i < ids.size(); ++i) {
    if (i != 0) out += ',';
    out += ids[i].str();
  }
  out += '}';
  return out;
}

// text[] literal with every element double-quoted and escaped.
std::string TextArray(const std::vector<std::string>& values) {
  std::string out = "{";
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i != 0) out += ',';
    out += '"';
    for (const char c : values[i]) {
      if (c == '"' || c == '\\') out += '\\';
      out += c;
    }
    out += '"';
  }
  out += '}';
  return out;
}

std::vector<JobId> IdColumn(const PgResult& result) {
  std::vector<JobId> ids;
  ids.reserve(result.rows.size());
  for (std::size_t i = 0; i < result.rows.size(); ++i) ids.push_back(JobId::Parse(result.Value(i, 0)));
  return ids;
}

std::vector<DependencyEdge> EdgeRows(const PgResult& result) {
  std::vector<DependencyEdge> edges;
  edges.reserve(result.rows.size());
  for (std::size_t i = 0; i < result.rows.size(); ++i) {
    edges.push_back({JobId::Parse(result.Value(i, 0)), JobId::Parse(result.Value(i, 1))});
  }
  return edges;
}

bool IsFailureKind(JobState state) {
  return state == JobState::kFailed || state == JobState::kTimeout;
}

std::string ReasonFor(JobState prerequisite_state, const JobId& prerequisite) {
  switch (prerequisite_state) {
    case JobState::kTimeout: return "prerequisite " + prerequisite.str() + " timed out";
    case JobState::kCancelled: return "prerequisite " + prerequisite.str() + " was cancelled";
    default: return "prerequisite " + prerequisite.str() + " failed";
  }
}

}  // namespace

void TakeGraphLock(PgConnection& connection) {
  connection.Query(std::string("SELECT pg_advisory_xact_lock(") + kGraphLockKey + ")");
}

std::vector<LockedJob> LockJobs(PgConnection& connection, std::vector<JobId> ids) {
  std::sort(ids.begin(), ids.end(), [](const JobId& a, const JobId& b) { return a.str() < b.str(); });
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
  const auto result = connection.Query(
      "SELECT id::text, status FROM jobs WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE", {UuidArray(ids)});
  std::vector<LockedJob> locked;
  std::set<std::string> found;
  for (std::size_t i = 0; i < result.rows.size(); ++i) {
    locked.push_back({JobId::Parse(result.Value(i, 0)), ParseJobState(result.Value(i, 1))});
    found.insert(result.Value(i, 0));
  }
  if (locked.size() != ids.size()) {
    std::string missing;
    for (const JobId& id : ids) {
      if (!found.count(id.str())) missing += (missing.empty() ? "" : ", ") + id.str();
    }
    throw NotFoundError("job(s) do not exist: " + missing);
  }
  return locked;
}

namespace {

std::vector<DependencyEdge> LoadClosure(PgConnection& connection, const std::vector<JobId>& roots,
                                        std::size_t max_edges, bool towards_prerequisites) {
  if (roots.empty()) return {};
  // Walk "waits for" edges (ancestors) or the reverse (descendants). UNION removes repeats, so
  // the walk terminates even on a graph that already contained a cycle.
  const char* start = towards_prerequisites ? "d.job_id = ANY($1::uuid[])" : "d.depends_on_id = ANY($1::uuid[])";
  const char* step = towards_prerequisites ? "d.job_id = w.depends_on_id" : "d.depends_on_id = w.job_id";
  const std::string sql =
      std::string("WITH RECURSIVE walk(job_id, depends_on_id) AS ("
                  "  SELECT d.job_id, d.depends_on_id FROM job_dependencies d WHERE ") + start +
      "  UNION"
      "  SELECT d.job_id, d.depends_on_id FROM job_dependencies d JOIN walk w ON " + step +
      ") SELECT job_id::text, depends_on_id::text FROM walk LIMIT $2::bigint";
  const auto result = connection.Query(sql, {UuidArray(roots), std::to_string(max_edges + 1)});
  if (result.rows.size() > max_edges) {
    throw ValidationError("dependency graph is too large to validate: more than " + std::to_string(max_edges) +
                          " edges are reachable");
  }
  return EdgeRows(result);
}

}  // namespace

std::vector<DependencyEdge> LoadAncestorEdges(PgConnection& connection, const std::vector<JobId>& roots,
                                              std::size_t max_edges) {
  return LoadClosure(connection, roots, max_edges, true);
}

std::vector<DependencyEdge> LoadDescendantEdges(PgConnection& connection, const std::vector<JobId>& roots,
                                                std::size_t max_edges) {
  return LoadClosure(connection, roots, max_edges, false);
}

void InsertEdges(PgConnection& connection, const std::vector<DependencyEdge>& edges) {
  if (edges.empty()) return;
  std::vector<JobId> jobs, prerequisites;
  for (const auto& edge : edges) {
    jobs.push_back(edge.job);
    prerequisites.push_back(edge.depends_on);
  }
  connection.Query(
      "INSERT INTO job_dependencies (job_id, depends_on_id) SELECT * FROM unnest($1::uuid[], $2::uuid[])",
      {UuidArray(jobs), UuidArray(prerequisites)});
}

std::vector<JobId> Prerequisites(PgConnection& connection, const JobId& job) {
  return IdColumn(connection.Query(
      "SELECT depends_on_id::text FROM job_dependencies WHERE job_id = $1::uuid ORDER BY depends_on_id",
      {job.str()}));
}

std::vector<JobId> Dependents(PgConnection& connection, const JobId& job) {
  return IdColumn(connection.Query(
      "SELECT job_id::text FROM job_dependencies WHERE depends_on_id = $1::uuid ORDER BY job_id", {job.str()}));
}

std::vector<JobId> UnmetPrerequisites(PgConnection& connection, const JobId& job) {
  return IdColumn(connection.Query(
      "SELECT p.id::text FROM job_dependencies d JOIN jobs p ON p.id = d.depends_on_id "
      "WHERE d.job_id = $1::uuid AND p.status <> 'COMPLETED' ORDER BY p.id",
      {job.str()}));
}

std::vector<JobId> ReleasableDependents(PgConnection& connection, const JobId& prerequisite) {
  return IdColumn(connection.Query(
      "SELECT d.job_id::text FROM job_dependencies d JOIN jobs j ON j.id = d.job_id "
      "WHERE d.depends_on_id = $1::uuid AND j.status = 'PENDING' AND NOT EXISTS ("
      "  SELECT 1 FROM job_dependencies d2 JOIN jobs p ON p.id = d2.depends_on_id "
      "  WHERE d2.job_id = d.job_id AND p.status <> 'COMPLETED') "
      "ORDER BY d.job_id",
      {prerequisite.str()}));
}

std::vector<Propagated> PropagateFailure(PgConnection& connection, const JobId& origin, JobState origin_state) {
  std::vector<Propagated> ended;
  struct Item {
    JobId id;
    JobState state;
  };
  std::vector<Item> frontier{{origin, origin_state}};

  while (!frontier.empty()) {
    std::vector<JobId> parent_ids;
    std::map<std::string, JobState> parent_state;
    for (const auto& item : frontier) {
      parent_ids.push_back(item.id);
      parent_state[item.id.str()] = item.state;
    }

    // PENDING dependents of this level, locked in id order (consistent lock order).
    const auto rows = connection.Query(
        "SELECT d.job_id::text, d.depends_on_id::text FROM job_dependencies d "
        "JOIN jobs j ON j.id = d.job_id "
        "WHERE d.depends_on_id = ANY($1::uuid[]) AND j.status = 'PENDING' "
        "ORDER BY d.job_id, d.depends_on_id FOR UPDATE OF j",
        {UuidArray(parent_ids)});
    if (rows.empty()) break;

    // Decide each dependent: FAILED if any prerequisite failed, otherwise CANCELLED.
    struct Decision {
      JobId job;
      JobState target = JobState::kCancelled;
      JobId caused_by;
      bool decided_by_failure = false;
    };
    std::vector<Decision> decisions;
    for (std::size_t i = 0; i < rows.rows.size(); ++i) {
      const JobId child = JobId::Parse(rows.Value(i, 0));
      const JobId parent = JobId::Parse(rows.Value(i, 1));
      const JobState parent_kind = parent_state.at(parent.str());
      if (decisions.empty() || !(decisions.back().job == child)) {
        decisions.push_back({child, JobState::kCancelled, parent, false});
      }
      Decision& decision = decisions.back();
      if (IsFailureKind(parent_kind) && !decision.decided_by_failure) {
        decision.target = JobState::kFailed;
        decision.caused_by = parent;
        decision.decided_by_failure = true;
      }
    }

    std::vector<JobId> ids;
    std::vector<std::string> targets, reasons;
    for (const auto& decision : decisions) {
      ids.push_back(decision.job);
      targets.emplace_back(ToString(decision.target));
      reasons.push_back(ReasonFor(parent_state.at(decision.caused_by.str()), decision.caused_by));
    }
    const std::vector<PgConnection::Param> params{UuidArray(ids), TextArray(targets), TextArray(reasons)};
    connection.Query(
        "UPDATE jobs j SET status = t.target, last_error = t.reason, finished_at = clock_timestamp() "
        "FROM unnest($1::uuid[], $2::text[], $3::text[]) AS t(id, target, reason) "
        "WHERE j.id = t.id AND j.status = 'PENDING'",
        params);
    connection.Query(
        "INSERT INTO job_events (job_id, from_status, to_status, reason) "
        "SELECT id, 'PENDING', target, reason FROM unnest($1::uuid[], $2::text[], $3::text[]) "
        "AS t(id, target, reason)",
        params);

    frontier.clear();
    for (const auto& decision : decisions) {
      ended.push_back({decision.job, decision.target, decision.caused_by});
      frontier.push_back({decision.job, decision.target});
    }
  }
  return ended;
}

}  // namespace taskflow::dependency_sql
