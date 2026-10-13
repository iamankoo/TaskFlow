#include "taskflow/dag/dependency_graph.hpp"

#include <algorithm>
#include <deque>

#include "taskflow/common/errors.hpp"

namespace taskflow {

std::size_t DependencyGraph::Index(const JobId& job) {
  const auto found = index_of_.find(job.str());
  if (found != index_of_.end()) return found->second;
  const std::size_t index = ids_.size();
  ids_.push_back(job);
  index_of_.emplace(job.str(), index);
  prerequisites_.emplace_back();
  dependents_.emplace_back();
  return index;
}

const std::size_t* DependencyGraph::Find(const JobId& job) const {
  const auto found = index_of_.find(job.str());
  return found == index_of_.end() ? nullptr : &found->second;
}

void DependencyGraph::AddNode(const JobId& job) { Index(job); }

bool DependencyGraph::AddEdge(const JobId& dependent, const JobId& prerequisite) {
  if (dependent.empty() || prerequisite.empty()) throw ValidationError("dependency edge has an empty job id");
  if (dependent == prerequisite) {
    throw ValidationError("job " + dependent.str() + " cannot depend on itself");
  }
  const std::size_t d = Index(dependent);
  const std::size_t p = Index(prerequisite);
  if (!edge_keys_.insert((static_cast<std::uint64_t>(d) << 32) | static_cast<std::uint64_t>(p)).second) {
    return false;  // duplicate
  }
  prerequisites_[d].push_back(p);
  dependents_[p].push_back(d);
  ++edge_count_;
  return true;
}

bool DependencyGraph::Contains(const JobId& job) const { return Find(job) != nullptr; }

bool DependencyGraph::HasEdge(const JobId& dependent, const JobId& prerequisite) const {
  const auto* d = Find(dependent);
  const auto* p = Find(prerequisite);
  if (!d || !p) return false;
  return edge_keys_.count((static_cast<std::uint64_t>(*d) << 32) | static_cast<std::uint64_t>(*p)) > 0;
}

std::vector<JobId> DependencyGraph::Prerequisites(const JobId& job) const {
  std::vector<JobId> out;
  if (const auto* index = Find(job)) {
    for (const std::size_t p : prerequisites_[*index]) out.push_back(ids_[p]);
  }
  return out;
}

std::vector<JobId> DependencyGraph::Dependents(const JobId& job) const {
  std::vector<JobId> out;
  if (const auto* index = Find(job)) {
    for (const std::size_t d : dependents_[*index]) out.push_back(ids_[d]);
  }
  return out;
}

std::vector<JobId> DependencyGraph::Reach(const JobId& job,
                                          const std::vector<std::vector<std::size_t>>& adjacency) const {
  std::vector<JobId> out;
  const auto* start = Find(job);
  if (!start) return out;
  std::vector<bool> seen(ids_.size(), false);
  std::deque<std::size_t> queue{*start};
  seen[*start] = true;
  while (!queue.empty()) {
    const std::size_t current = queue.front();
    queue.pop_front();
    for (const std::size_t next : adjacency[current]) {
      if (seen[next]) continue;
      seen[next] = true;
      out.push_back(ids_[next]);
      queue.push_back(next);
    }
  }
  return out;
}

std::vector<JobId> DependencyGraph::Ancestors(const JobId& job) const { return Reach(job, prerequisites_); }
std::vector<JobId> DependencyGraph::Descendants(const JobId& job) const { return Reach(job, dependents_); }

std::optional<std::vector<JobId>> DependencyGraph::FindCycle() const {
  enum : unsigned char { kUnvisited = 0, kOnPath = 1, kDone = 2 };
  std::vector<unsigned char> state(ids_.size(), kUnvisited);
  struct Frame {
    std::size_t node;
    std::size_t next_child;  // position in prerequisites_[node]
  };
  std::vector<Frame> stack;  // the current path, explicit so depth is bounded by memory, not the call stack

  for (std::size_t root = 0; root < ids_.size(); ++root) {
    if (state[root] != kUnvisited) continue;
    stack.push_back({root, 0});
    state[root] = kOnPath;
    while (!stack.empty()) {
      Frame& frame = stack.back();
      if (frame.next_child < prerequisites_[frame.node].size()) {
        const std::size_t child = prerequisites_[frame.node][frame.next_child++];
        if (state[child] == kOnPath) {  // back edge: the path from `child` to here is a cycle
          std::vector<JobId> cycle;
          auto begin = std::find_if(stack.begin(), stack.end(), [&](const Frame& f) { return f.node == child; });
          for (auto it = begin; it != stack.end(); ++it) cycle.push_back(ids_[it->node]);
          cycle.push_back(ids_[child]);
          return cycle;
        }
        if (state[child] == kUnvisited) {
          state[child] = kOnPath;
          stack.push_back({child, 0});  // may invalidate `frame`; not used again this iteration
        }
      } else {
        state[frame.node] = kDone;
        stack.pop_back();
      }
    }
  }
  return std::nullopt;
}

std::vector<JobId> DependencyGraph::TopologicalOrder() const {
  const std::size_t count = ids_.size();
  std::vector<std::size_t> waiting_for(count);
  std::deque<std::size_t> ready;
  for (std::size_t i = 0; i < count; ++i) {
    waiting_for[i] = prerequisites_[i].size();
    if (waiting_for[i] == 0) ready.push_back(i);  // roots, in insertion order
  }
  std::vector<JobId> order;
  order.reserve(count);
  while (!ready.empty()) {
    const std::size_t current = ready.front();
    ready.pop_front();
    order.push_back(ids_[current]);
    for (const std::size_t dependent : dependents_[current]) {
      if (--waiting_for[dependent] == 0) ready.push_back(dependent);
    }
  }
  if (order.size() != count) {
    const auto cycle = FindCycle();
    std::vector<std::string> path;
    std::string text;
    if (cycle) {
      for (std::size_t i = 0; i < cycle->size(); ++i) {
        path.push_back((*cycle)[i].str());
        text += (i ? " -> " : "") + (*cycle)[i].str();
      }
    }
    throw CycleError("dependency cycle: " + text, std::move(path));
  }
  return order;
}

}  // namespace taskflow
