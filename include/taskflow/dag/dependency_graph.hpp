#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "taskflow/jobs/job.hpp"

namespace taskflow {

// "job" waits for "depends_on": the edge job -> depends_on in the job's own terms, or
// depends_on -> job in the direction work flows.
struct DependencyEdge {
  JobId job;         // the dependent
  JobId depends_on;  // the prerequisite
  friend bool operator==(const DependencyEdge&, const DependencyEdge&) = default;
};

// Limits that keep a single request and a single cycle check bounded.
inline constexpr std::size_t kMaxPrerequisitesPerJob = 1000;
inline constexpr std::size_t kMaxEdgesPerRequest = 100'000;
inline constexpr std::size_t kMaxWorkflowJobs = 10'000;
inline constexpr std::size_t kMaxGraphClosureEdges = 500'000;  // ancestors loaded for one cycle check

// In-memory directed graph of jobs used to validate a proposed set of dependencies.
// Pure domain logic: no I/O, no recursion (every traversal uses an explicit stack or
// queue, so a 100,000-job chain cannot overflow the call stack).
//
// Complexity, with V nodes and E edges (hash-map lookups counted as O(1) expected):
//   AddEdge                    O(1)
//   FindCycle                  O(V + E)    iterative depth-first search
//   TopologicalOrder           O(V + E)    Kahn's algorithm with a FIFO queue
//   Ancestors / Descendants    O(V + E) of the part reachable
// Order is deterministic: ties are resolved by the order in which nodes were first added.
class DependencyGraph {
 public:
  // Adds "dependent waits for prerequisite". Nodes are created on first mention. Throws
  // ValidationError for a self-dependency. Returns false (and changes nothing) when the
  // edge is already present.
  bool AddEdge(const JobId& dependent, const JobId& prerequisite);
  void AddNode(const JobId& job);

  std::size_t node_count() const noexcept { return ids_.size(); }
  std::size_t edge_count() const noexcept { return edge_count_; }
  bool Contains(const JobId& job) const;
  bool HasEdge(const JobId& dependent, const JobId& prerequisite) const;

  // Direct neighbours, in insertion order. Empty for unknown jobs.
  std::vector<JobId> Prerequisites(const JobId& job) const;
  std::vector<JobId> Dependents(const JobId& job) const;

  // Everything `job` transitively waits for / everything that transitively waits for `job`
  // (excluding `job` itself), in breadth-first order.
  std::vector<JobId> Ancestors(const JobId& job) const;
  std::vector<JobId> Descendants(const JobId& job) const;

  // A cycle as a closed path (a, b, c, a) meaning a waits for b waits for c waits for a, or
  // nullopt when the graph is acyclic.
  std::optional<std::vector<JobId>> FindCycle() const;

  // Prerequisites before dependents: a valid execution order. Throws CycleError when the
  // graph has a cycle.
  std::vector<JobId> TopologicalOrder() const;

 private:
  std::size_t Index(const JobId& job);
  const std::size_t* Find(const JobId& job) const;
  std::vector<JobId> Reach(const JobId& job, const std::vector<std::vector<std::size_t>>& adjacency) const;

  std::vector<JobId> ids_;                                   // index -> job
  std::unordered_map<std::string, std::size_t> index_of_;    // job -> index
  std::vector<std::vector<std::size_t>> prerequisites_;      // index -> prerequisite indices
  std::vector<std::vector<std::size_t>> dependents_;         // index -> dependent indices
  std::unordered_set<std::uint64_t> edge_keys_;              // (dependent, prerequisite) pairs: O(1) duplicate check
  std::size_t edge_count_ = 0;
};

}  // namespace taskflow
