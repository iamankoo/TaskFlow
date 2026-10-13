#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <map>
#include <set>

#include "taskflow/common/errors.hpp"
#include "taskflow/dag/dependency_graph.hpp"

namespace taskflow {
namespace {

JobId Id(std::int64_t n) {
  char buffer[40];
  std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0000-%012lld", static_cast<long long>(n));
  return JobId::Parse(buffer);
}

// Edge helper: `job` waits for `prerequisite`.
void Waits(DependencyGraph& graph, std::int64_t job, std::int64_t prerequisite) {
  graph.AddEdge(Id(job), Id(prerequisite));
}

std::vector<std::int64_t> Numbers(const std::vector<JobId>& ids) {
  std::vector<std::int64_t> out;
  for (const auto& id : ids) out.push_back(std::stoll(id.str().substr(24)));
  return out;
}

std::map<std::int64_t, std::size_t> Positions(const std::vector<JobId>& order) {
  std::map<std::int64_t, std::size_t> position;
  const auto numbers = Numbers(order);
  for (std::size_t i = 0; i < numbers.size(); ++i) position[numbers[i]] = i;
  return position;
}

// A topological order is valid when every prerequisite comes before the job that waits for it.
void ExpectValidOrder(const DependencyGraph& graph, const std::vector<JobId>& order) {
  ASSERT_EQ(order.size(), graph.node_count());
  const auto position = Positions(order);
  for (const auto& id : order) {
    for (const auto& prerequisite : graph.Prerequisites(id)) {
      EXPECT_LT(position.at(std::stoll(prerequisite.str().substr(24))), position.at(std::stoll(id.str().substr(24))))
          << prerequisite.str() << " must precede " << id.str();
    }
  }
}

// ---------- small shapes ----------

TEST(DependencyGraphTest, EmptyGraph) {
  const DependencyGraph graph;
  EXPECT_EQ(graph.node_count(), 0u);
  EXPECT_EQ(graph.edge_count(), 0u);
  EXPECT_TRUE(graph.TopologicalOrder().empty());
  EXPECT_FALSE(graph.FindCycle().has_value());
  EXPECT_TRUE(graph.Ancestors(Id(1)).empty());
}

TEST(DependencyGraphTest, SingleNode) {
  DependencyGraph graph;
  graph.AddNode(Id(1));
  EXPECT_EQ(graph.node_count(), 1u);
  EXPECT_EQ(Numbers(graph.TopologicalOrder()), (std::vector<std::int64_t>{1}));
  EXPECT_FALSE(graph.FindCycle().has_value());
  EXPECT_TRUE(graph.Contains(Id(1)));
}

TEST(DependencyGraphTest, LinearChainOrdersPrerequisitesFirst) {
  DependencyGraph graph;  // 5 waits for 4 waits for 3 waits for 2 waits for 1
  for (std::int64_t i = 5; i >= 2; --i) Waits(graph, i, i - 1);
  EXPECT_EQ(Numbers(graph.TopologicalOrder()), (std::vector<std::int64_t>{1, 2, 3, 4, 5}));
  EXPECT_EQ(Numbers(graph.Ancestors(Id(5))), (std::vector<std::int64_t>{4, 3, 2, 1}));
  EXPECT_EQ(Numbers(graph.Descendants(Id(1))), (std::vector<std::int64_t>{2, 3, 4, 5}));
}

TEST(DependencyGraphTest, DiamondFromTheSpecification) {
  // A: fetch -> B, C -> D: report -> E: publish
  DependencyGraph graph;
  const std::int64_t A = 1, B = 2, C = 3, D = 4, E = 5;
  Waits(graph, B, A);
  Waits(graph, C, A);
  Waits(graph, D, B);
  Waits(graph, D, C);
  Waits(graph, E, D);

  EXPECT_EQ(graph.node_count(), 5u);
  EXPECT_EQ(graph.edge_count(), 5u);
  EXPECT_FALSE(graph.FindCycle().has_value());
  const auto order = graph.TopologicalOrder();
  ExpectValidOrder(graph, order);
  EXPECT_EQ(Numbers(order).front(), A);
  EXPECT_EQ(Numbers(order).back(), E);
  EXPECT_EQ(Numbers(graph.Prerequisites(Id(D))), (std::vector<std::int64_t>{B, C}));
  EXPECT_EQ(Numbers(graph.Dependents(Id(A))), (std::vector<std::int64_t>{B, C}));
  const auto ancestors = Numbers(graph.Ancestors(Id(E)));
  EXPECT_EQ(std::set<std::int64_t>(ancestors.begin(), ancestors.end()), (std::set<std::int64_t>{A, B, C, D}));
}

TEST(DependencyGraphTest, MultipleParentsAllPrecedeTheChild) {
  DependencyGraph graph;
  for (std::int64_t parent = 1; parent <= 6; ++parent) Waits(graph, 100, parent);
  const auto order = graph.TopologicalOrder();
  ExpectValidOrder(graph, order);
  EXPECT_EQ(Numbers(order).back(), 100);
}

TEST(DependencyGraphTest, IndependentComponentsStayIndependent) {
  DependencyGraph graph;
  Waits(graph, 2, 1);
  Waits(graph, 12, 11);
  graph.AddNode(Id(99));  // an isolated job
  const auto order = graph.TopologicalOrder();
  ExpectValidOrder(graph, order);
  EXPECT_EQ(order.size(), 5u);
  EXPECT_TRUE(graph.Ancestors(Id(2)).size() == 1 && graph.Descendants(Id(11)).size() == 1);
  EXPECT_TRUE(graph.Ancestors(Id(99)).empty());
}

// ---------- invalid input ----------

TEST(DependencyGraphTest, SelfDependencyIsRejected) {
  DependencyGraph graph;
  EXPECT_THROW(graph.AddEdge(Id(1), Id(1)), ValidationError);
  EXPECT_EQ(graph.edge_count(), 0u);
  EXPECT_THROW(graph.AddEdge(JobId{}, Id(1)), ValidationError);  // empty id
}

TEST(DependencyGraphTest, DuplicateEdgesAreDetectedAndNotCounted) {
  DependencyGraph graph;
  EXPECT_TRUE(graph.AddEdge(Id(2), Id(1)));
  EXPECT_FALSE(graph.AddEdge(Id(2), Id(1)));
  EXPECT_EQ(graph.edge_count(), 1u);
  EXPECT_EQ(graph.Prerequisites(Id(2)).size(), 1u);
  EXPECT_TRUE(graph.AddEdge(Id(1), Id(2)));  // the opposite direction is a different edge
  EXPECT_TRUE(graph.HasEdge(Id(2), Id(1)));
}

TEST(DependencyGraphTest, UnknownNodesAreEmptyNotErrors) {
  DependencyGraph graph;
  Waits(graph, 2, 1);
  EXPECT_FALSE(graph.Contains(Id(77)));
  EXPECT_TRUE(graph.Prerequisites(Id(77)).empty());
  EXPECT_TRUE(graph.Dependents(Id(77)).empty());
  EXPECT_TRUE(graph.Ancestors(Id(77)).empty());
  EXPECT_FALSE(graph.HasEdge(Id(77), Id(1)));
}

// ---------- cycles ----------

void ExpectClosedPathOfEdges(const DependencyGraph& graph, const std::vector<JobId>& cycle) {
  ASSERT_GE(cycle.size(), 3u);  // at least a -> b -> a
  EXPECT_EQ(cycle.front(), cycle.back());
  for (std::size_t i = 0; i + 1 < cycle.size(); ++i) {
    EXPECT_TRUE(graph.HasEdge(cycle[i], cycle[i + 1])) << "cycle step " << i << " is not an edge";
  }
}

TEST(DependencyGraphTest, DirectCycleIsFound) {
  DependencyGraph graph;
  Waits(graph, 1, 2);
  Waits(graph, 2, 1);
  const auto cycle = graph.FindCycle();
  ASSERT_TRUE(cycle.has_value());
  ExpectClosedPathOfEdges(graph, *cycle);
  EXPECT_EQ(cycle->size(), 3u);
}

TEST(DependencyGraphTest, IndirectCycleIsFoundWithItsPath) {
  DependencyGraph graph;
  Waits(graph, 1, 2);
  Waits(graph, 2, 3);
  Waits(graph, 3, 4);
  Waits(graph, 4, 1);  // closes a four-job cycle
  Waits(graph, 10, 1);  // a bystander that waits for the cycle but is not part of it
  const auto cycle = graph.FindCycle();
  ASSERT_TRUE(cycle.has_value());
  ExpectClosedPathOfEdges(graph, *cycle);
  EXPECT_EQ(cycle->size(), 5u);
  EXPECT_EQ(std::count_if(cycle->begin(), cycle->end(), [](const JobId& id) { return id == Id(10); }), 0);
}

TEST(DependencyGraphTest, CycleInASecondComponentIsFound) {
  DependencyGraph graph;
  Waits(graph, 2, 1);  // acyclic component
  Waits(graph, 12, 11);
  Waits(graph, 13, 12);
  Waits(graph, 11, 13);  // cyclic component
  EXPECT_TRUE(graph.FindCycle().has_value());
}

TEST(DependencyGraphTest, TopologicalOrderOfACyclicGraphThrowsWithThePath) {
  DependencyGraph graph;
  Waits(graph, 1, 2);
  Waits(graph, 2, 3);
  Waits(graph, 3, 1);
  try {
    graph.TopologicalOrder();
    FAIL() << "expected CycleError";
  } catch (const CycleError& e) {
    EXPECT_GE(e.cycle().size(), 4u);
    EXPECT_EQ(e.cycle().front(), e.cycle().back());
    EXPECT_NE(std::string(e.what()).find("cycle"), std::string::npos);
    EXPECT_EQ(e.code(), ErrorCode::kInvalidArgument);  // a kind of ValidationError
  }
}

TEST(DependencyGraphTest, AcyclicGraphsReportNoCycle) {
  DependencyGraph graph;
  for (std::int64_t i = 2; i <= 50; ++i) {
    Waits(graph, i, i - 1);
    if (i > 2) Waits(graph, i, i - 2);  // shortcuts do not make a cycle
  }
  EXPECT_FALSE(graph.FindCycle().has_value());
}

// ---------- determinism ----------

TEST(DependencyGraphTest, OrderIsDeterministicForTheSameInsertionSequence) {
  const auto build = [] {
    DependencyGraph graph;
    Waits(graph, 5, 1);
    Waits(graph, 6, 2);
    Waits(graph, 7, 5);
    Waits(graph, 7, 6);
    graph.AddNode(Id(3));
    return graph.TopologicalOrder();
  };
  EXPECT_EQ(Numbers(build()), Numbers(build()));
  // Ties are broken by insertion order: roots appear in the order they were first mentioned.
  EXPECT_EQ(Numbers(build()), (std::vector<std::int64_t>{1, 2, 3, 5, 6, 7}));
}

// ---------- large graphs (no recursion, linear work) ----------

TEST(DependencyGraphTest, ALongChainDoesNotOverflowTheStack) {
  constexpr std::int64_t kLength = 200'000;
  DependencyGraph graph;
  for (std::int64_t i = 2; i <= kLength; ++i) Waits(graph, i, i - 1);
  EXPECT_EQ(graph.node_count(), static_cast<std::size_t>(kLength));
  EXPECT_FALSE(graph.FindCycle().has_value());
  const auto order = graph.TopologicalOrder();
  ASSERT_EQ(order.size(), static_cast<std::size_t>(kLength));
  EXPECT_EQ(Numbers({order.front()}).front(), 1);
  EXPECT_EQ(Numbers({order.back()}).front(), kLength);
  EXPECT_EQ(graph.Ancestors(Id(kLength)).size(), static_cast<std::size_t>(kLength - 1));
  EXPECT_EQ(graph.Descendants(Id(1)).size(), static_cast<std::size_t>(kLength - 1));
}

TEST(DependencyGraphTest, ALongChainClosedIntoACycleIsDetected) {
  constexpr std::int64_t kLength = 200'000;
  DependencyGraph graph;
  for (std::int64_t i = 2; i <= kLength; ++i) Waits(graph, i, i - 1);
  Waits(graph, 1, kLength);  // the last job is a prerequisite of the first
  const auto cycle = graph.FindCycle();
  ASSERT_TRUE(cycle.has_value());
  EXPECT_EQ(cycle->size(), static_cast<std::size_t>(kLength + 1));
  EXPECT_THROW(graph.TopologicalOrder(), CycleError);
}

TEST(DependencyGraphTest, WideAndLayeredGraphsOrderCorrectly) {
  DependencyGraph wide;  // one root, 100,000 dependents, one sink waiting for all of them
  constexpr std::int64_t kWidth = 100'000;
  for (std::int64_t i = 2; i <= kWidth + 1; ++i) {
    Waits(wide, i, 1);
    Waits(wide, kWidth + 2, i);
  }
  EXPECT_EQ(wide.edge_count(), static_cast<std::size_t>(2 * kWidth));
  ExpectValidOrder(wide, wide.TopologicalOrder());

  DependencyGraph layered;  // 60 layers x 500 jobs; each job waits for 3 jobs of the previous layer
  constexpr std::int64_t kLayers = 60, kPerLayer = 500;
  for (std::int64_t layer = 1; layer < kLayers; ++layer) {
    for (std::int64_t i = 0; i < kPerLayer; ++i) {
      const std::int64_t job = layer * kPerLayer + i + 1;
      for (const std::int64_t k : {0, 1, 7}) Waits(layered, job, (layer - 1) * kPerLayer + (i + k) % kPerLayer + 1);
    }
  }
  EXPECT_FALSE(layered.FindCycle().has_value());
  ExpectValidOrder(layered, layered.TopologicalOrder());
}

TEST(DependencyGraphTest, ValidationOfAHundredThousandEdgesIsFast) {
  DependencyGraph graph;
  for (std::int64_t i = 2; i <= 100'001; ++i) Waits(graph, i, i / 2);  // a binary tree of prerequisites
  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(graph.FindCycle().has_value());
  const auto order = graph.TopologicalOrder();
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(order.size(), 100'001u);
  EXPECT_LT(elapsed, std::chrono::seconds(10)) << "a generous bound: this is linear work";
}

}  // namespace
}  // namespace taskflow
