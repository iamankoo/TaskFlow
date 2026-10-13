#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>

#include "dag_fixture.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;
using testing::DagFixture;

class DependencyServiceTest : public DagFixture {
 protected:
  std::set<std::string> Names(const std::vector<JobId>& ids) {
    std::set<std::string> out;
    for (const auto& id : ids) out.insert(id.str());
    return out;
  }
  std::set<std::string> Names(std::initializer_list<SchedulingEntry> entries) {
    std::set<std::string> out;
    for (const auto& e : entries) out.insert(e.id.str());
    return out;
  }

  // Runs two operations at the same moment on two threads.
  template <typename A, typename B>
  void Race(A&& first, B&& second) {
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    const auto sync = [&] {
      ++ready;
      while (!go.load()) std::this_thread::yield();
    };
    std::thread t1([&] { sync(); first(); });
    std::thread t2([&] { sync(); second(); });
    while (ready.load() < 2) std::this_thread::yield();
    go.store(true);
    t1.join();
    t2.join();
  }
};

// ---------- storing and reading edges ----------

TEST_F(DependencyServiceTest, DependenciesArePersistedAndQueryable) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{c.id, a.id}, {c.id, b.id}});

  EXPECT_EQ(Names(deps_->Prerequisites(c.id)), Names({a, b}));
  EXPECT_EQ(Names(deps_->Dependents(a.id)), Names({c}));
  EXPECT_TRUE(deps_->Prerequisites(a.id).empty());
  EXPECT_EQ(Count("job_dependencies"), 2);

  DependencyService fresh(*pool_, *repo_);  // a different service instance sees the same durable state
  EXPECT_EQ(Names(fresh.Prerequisites(c.id)), Names({a, b}));
}

TEST_F(DependencyServiceTest, InvalidRequestsAreRejectedBeforeAnythingIsStored) {
  const auto a = Due(), b = Due();
  EXPECT_THROW(deps_->AddDependencies({}), ValidationError);
  EXPECT_THROW(deps_->AddDependencies({{a.id, a.id}}), ValidationError);                    // self-dependency
  EXPECT_THROW(deps_->AddDependencies({{b.id, a.id}, {b.id, a.id}}), ValidationError);       // duplicate in request
  EXPECT_THROW(deps_->AddDependencies({{b.id, JobId::Parse("99999999-9999-9999-9999-999999999999")}}),
               NotFoundError);                                                               // missing job
  EXPECT_EQ(Count("job_dependencies"), 0);
}

TEST_F(DependencyServiceTest, AnEdgeThatIsAlreadyStoredIsAConflict) {
  const auto a = Due(), b = Due();
  deps_->AddDependencies({{b.id, a.id}});
  EXPECT_THROW(deps_->AddDependencies({{b.id, a.id}}), ConflictError);
  EXPECT_EQ(Count("job_dependencies"), 1);
}

TEST_F(DependencyServiceTest, AMultiEdgeRequestIsAtomic) {
  const auto a = Due(), b = Due(), c = Due(), d = Due();
  deps_->AddDependencies({{d.id, a.id}});
  // Three new edges, the last of which already exists: none of them may be stored.
  EXPECT_THROW(deps_->AddDependencies({{b.id, a.id}, {c.id, a.id}, {d.id, a.id}}), ConflictError);
  EXPECT_EQ(Count("job_dependencies"), 1);
  // The same when the last one names a missing job.
  EXPECT_THROW(deps_->AddDependencies({{b.id, a.id}, {c.id, JobId::Parse("99999999-9999-9999-9999-999999999999")}}),
               NotFoundError);
  EXPECT_EQ(Count("job_dependencies"), 1);
}

TEST_F(DependencyServiceTest, PrerequisitesCannotBeAddedToAJobThatWasAlreadyReleased) {
  const auto a = Due();
  const auto ready = Ready(JobPriority::kNormal, Ms(-1000));
  EXPECT_THROW(deps_->AddDependencies({{ready.id, a.id}}), ConflictError);
  EXPECT_EQ(Count("job_dependencies"), 0);
}

TEST_F(DependencyServiceTest, TooManyPrerequisitesForOneJobIsRejected) {
  std::vector<DependencyEdge> edges;
  const auto dependent = JobId::Parse("00000000-0000-0000-0000-000000000001");
  for (std::size_t i = 0; i <= kMaxPrerequisitesPerJob; ++i) {
    char buffer[40];
    std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0001-%012zu", i);
    edges.push_back({dependent, JobId::Parse(buffer)});
  }
  EXPECT_THROW(deps_->AddDependencies(edges), ValidationError);
}

// ---------- cycles ----------

TEST_F(DependencyServiceTest, ADirectCycleIsRejected) {
  const auto x = Due(), y = Due();
  deps_->AddDependencies({{x.id, y.id}});  // X waits for Y
  try {
    deps_->AddDependencies({{y.id, x.id}});  // Y waits for X: cycle
    FAIL() << "expected CycleError";
  } catch (const CycleError& e) {
    EXPECT_EQ(e.cycle().size(), 3u);
    EXPECT_EQ(e.cycle().front(), e.cycle().back());
  }
  EXPECT_EQ(Count("job_dependencies"), 1);
}

TEST_F(DependencyServiceTest, AnIndirectCycleIsRejected) {
  const auto a = Due(), b = Due(), c = Due(), d = Due();
  deps_->AddDependencies({{b.id, a.id}, {c.id, b.id}, {d.id, c.id}});  // a <- b <- c <- d
  EXPECT_THROW(deps_->AddDependencies({{a.id, d.id}}), CycleError);   // a waits for d: four-job cycle
  EXPECT_THROW(deps_->AddDependencies({{b.id, d.id}}), CycleError);
  EXPECT_EQ(Count("job_dependencies"), 3);
}

TEST_F(DependencyServiceTest, ACycleInsideASingleRequestIsRejectedAsAWhole) {
  const auto a = Due(), b = Due(), c = Due();
  EXPECT_THROW(deps_->AddDependencies({{a.id, b.id}, {b.id, c.id}, {c.id, a.id}}), CycleError);
  EXPECT_EQ(Count("job_dependencies"), 0);
}

TEST_F(DependencyServiceTest, DiamondsAndMultipleParentsAreNotCycles) {
  const auto a = Due(), b = Due(), c = Due(), d = Due(), e = Due();
  EXPECT_NO_THROW(deps_->AddDependencies({{b.id, a.id}, {c.id, a.id}}));
  EXPECT_NO_THROW(deps_->AddDependencies({{d.id, b.id}, {d.id, c.id}}));
  EXPECT_NO_THROW(deps_->AddDependencies({{e.id, d.id}, {e.id, a.id}}));  // shortcut a -> e
  EXPECT_EQ(Count("job_dependencies"), 6);  // b,c <- a; d <- b,c; e <- d,a
}

TEST_F(DependencyServiceTest, ValidateEdgesReportsWithoutStoring) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{b.id, a.id}});

  const GraphCheck ok = deps_->ValidateEdges({{c.id, b.id}});
  EXPECT_TRUE(ok.valid);
  EXPECT_EQ(ok.nodes, 3u);  // a, b (existing ancestors) and c
  EXPECT_EQ(ok.edges, 2u);
  ASSERT_EQ(ok.topological_order.size(), 3u);
  EXPECT_EQ(ok.topological_order.front(), a.id);
  EXPECT_EQ(ok.topological_order.back(), c.id);

  const GraphCheck bad = deps_->ValidateEdges({{a.id, c.id}, {c.id, b.id}});  // a <- b <- c <- a
  EXPECT_FALSE(bad.valid);
  EXPECT_GE(bad.cycle.size(), 4u);
  EXPECT_THROW(deps_->ValidateEdges({{a.id, a.id}}), ValidationError);
  EXPECT_THROW(deps_->ValidateEdges({{a.id, JobId::Parse("99999999-9999-9999-9999-999999999999")}}), NotFoundError);
  EXPECT_EQ(Count("job_dependencies"), 1);  // nothing stored by validation
}

// ---------- concurrent graph changes ----------

TEST_F(DependencyServiceTest, OppositeEdgesSubmittedAtTheSameTimeNeverBothSucceed) {
  for (int round = 0; round < 40; ++round) {
    const auto x = Due(), y = Due();
    std::atomic<int> ok{0}, cycles{0}, other{0};
    const auto attempt = [&](const SchedulingEntry& job, const SchedulingEntry& prerequisite) {
      return [&, job, prerequisite] {
        try {
          deps_->AddDependencies({{job.id, prerequisite.id}});
          ++ok;
        } catch (const CycleError&) {
          ++cycles;
        } catch (...) {
          ++other;
        }
      };
    };
    Race(attempt(x, y), attempt(y, x));  // X waits for Y  ||  Y waits for X
    ASSERT_EQ(ok.load(), 1) << "round " << round;
    ASSERT_EQ(cycles.load(), 1) << "round " << round;
    ASSERT_EQ(other.load(), 0) << "round " << round;
  }
  // Whatever happened, the stored graph is acyclic.
  DependencyGraph graph;
  for (const auto& edge : AllEdges()) graph.AddEdge(edge.job, edge.depends_on);
  EXPECT_FALSE(graph.FindCycle().has_value());
  EXPECT_EQ(Count("job_dependencies"), 40);  // exactly one edge per round
}

TEST_F(DependencyServiceTest, ManyThreadsClosingARingNeverCompleteIt) {
  constexpr int kRing = 8;
  for (int round = 0; round < 10; ++round) {
    std::vector<SchedulingEntry> jobs;
    for (int i = 0; i < kRing; ++i) jobs.push_back(Due());
    std::atomic<int> ok{0}, cycles{0}, other{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < kRing; ++i) {
      threads.emplace_back([&, i] {
        while (!go.load()) std::this_thread::yield();
        try {
          deps_->AddDependencies({{jobs[static_cast<std::size_t>(i)].id, jobs[static_cast<std::size_t>((i + 1) % kRing)].id}});
          ++ok;
        } catch (const CycleError&) {
          ++cycles;
        } catch (...) {
          ++other;
        }
      });
    }
    go.store(true);
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(other.load(), 0);
    EXPECT_EQ(ok.load() + cycles.load(), kRing);
    EXPECT_EQ(ok.load(), kRing - 1) << "exactly the edge that would close the ring is refused";
    EXPECT_EQ(cycles.load(), 1);
  }
  DependencyGraph graph;
  for (const auto& edge : AllEdges()) graph.AddEdge(edge.job, edge.depends_on);
  EXPECT_FALSE(graph.FindCycle().has_value());
}

TEST_F(DependencyServiceTest, AddingADependencyWhileItsPrerequisiteFailsNeverStrandsTheDependent) {
  for (int round = 0; round < 25; ++round) {
    const auto prerequisite = Due();
    const JobAttempt attempt = Start(prerequisite);
    const auto dependent = Due();
    std::atomic<int> errors{0};
    Race(
        [&] {
          try {
            AttemptOutcome failure;
            failure.state = AttemptState::kFailed;
            failure.error_message = "boom";
            repo_->StoreResult(prerequisite.id, attempt.attempt_number, failure, JobState::kFailed);
          } catch (...) {
            ++errors;
          }
        },
        [&] {
          try {
            deps_->AddDependencies({{dependent.id, prerequisite.id}});
          } catch (...) {
            ++errors;
          }
        });
    ASSERT_EQ(errors.load(), 0) << "round " << round;
    // Whichever came first, the dependent has a defined outcome: FAILED, never stuck PENDING.
    ASSERT_EQ(State(dependent), JobState::kFailed) << "round " << round;
    ASSERT_EQ(Count("job_dependencies", "job_id = '" + dependent.id.str() + "'"), 1) << "round " << round;
  }
}

// ---------- workflows ----------

WorkflowNode Node(const std::string& key, std::vector<std::string> depends_on = {}) {
  WorkflowNode node;
  node.key = key;
  node.job.type = "workflow." + key;
  node.depends_on = std::move(depends_on);
  return node;
}

TEST_F(DependencyServiceTest, ADiamondWorkflowIsCreatedAtomicallyInDependencyOrder) {
  // A: fetch -> B, C -> D: report -> E: publish
  WorkflowSpec spec;
  spec.nodes = {Node("E", {"D"}), Node("D", {"B", "C"}), Node("C", {"A"}), Node("B", {"A"}), Node("A")};
  const CreatedWorkflow created = deps_->CreateWorkflow(spec);

  EXPECT_EQ(created.jobs.size(), 5u);
  EXPECT_EQ(created.order.front(), "A");
  EXPECT_EQ(created.order.back(), "E");
  EXPECT_LT(created.jobs.at("A").sequence, created.jobs.at("D").sequence);  // created prerequisites first
  EXPECT_EQ(Count("jobs"), 5);
  EXPECT_EQ(Count("job_dependencies"), 5);
  EXPECT_EQ(Names(deps_->Prerequisites(created.jobs.at("D").id)),
            (std::set<std::string>{created.jobs.at("B").id.str(), created.jobs.at("C").id.str()}));
  for (const auto& [key, job] : created.jobs) EXPECT_EQ(job.state, JobState::kPending) << key;
  EXPECT_EQ(Count("job_events", "reason = 'job created'"), 5);
}

TEST_F(DependencyServiceTest, ARootOfAWorkflowMayStartReadyAndMayAlsoWaitForExistingJobs) {
  const auto existing = Due();
  WorkflowSpec spec;
  spec.nodes = {Node("root"), Node("child", {"root"})};
  spec.nodes[0].job.initial_state = JobState::kReady;
  spec.nodes[1].depends_on_existing = {existing.id};
  const CreatedWorkflow created = deps_->CreateWorkflow(spec);
  EXPECT_EQ(created.jobs.at("root").state, JobState::kReady);
  EXPECT_EQ(deps_->Prerequisites(created.jobs.at("child").id).size(), 2u);
}

TEST_F(DependencyServiceTest, InvalidWorkflowsStoreNothing) {
  const auto expect_nothing_stored = [&] {
    EXPECT_EQ(Count("jobs"), 0);
    EXPECT_EQ(Count("job_dependencies"), 0);
    EXPECT_EQ(Count("job_events"), 0);
  };
  WorkflowSpec cycle;
  cycle.nodes = {Node("a", {"c"}), Node("b", {"a"}), Node("c", {"b"})};
  try {
    deps_->CreateWorkflow(cycle);
    FAIL() << "expected CycleError";
  } catch (const CycleError& e) {
    EXPECT_NE(std::string(e.what()).find("->"), std::string::npos);
    for (const auto& key : e.cycle()) EXPECT_TRUE(key == "a" || key == "b" || key == "c") << key;  // caller's keys
  }
  expect_nothing_stored();

  WorkflowSpec unknown;
  unknown.nodes = {Node("a", {"missing"})};
  EXPECT_THROW(deps_->CreateWorkflow(unknown), ValidationError);
  WorkflowSpec duplicate_key;
  duplicate_key.nodes = {Node("a"), Node("a")};
  EXPECT_THROW(deps_->CreateWorkflow(duplicate_key), ValidationError);
  WorkflowSpec self;
  self.nodes = {Node("a", {"a"})};
  EXPECT_THROW(deps_->CreateWorkflow(self), ValidationError);
  WorkflowSpec listed_twice;
  listed_twice.nodes = {Node("a"), Node("b", {"a", "a"})};
  EXPECT_THROW(deps_->CreateWorkflow(listed_twice), ValidationError);
  WorkflowSpec ready_with_prerequisite;
  ready_with_prerequisite.nodes = {Node("a"), Node("b", {"a"})};
  ready_with_prerequisite.nodes[1].job.initial_state = JobState::kReady;
  EXPECT_THROW(deps_->CreateWorkflow(ready_with_prerequisite), ValidationError);
  EXPECT_THROW(deps_->CreateWorkflow(WorkflowSpec{}), ValidationError);
  expect_nothing_stored();
}

TEST_F(DependencyServiceTest, AWorkflowWithAnInvalidJobRollsBackTheJobsCreatedBeforeIt) {
  WorkflowSpec spec;
  spec.nodes = {Node("a"), Node("b", {"a"}), Node("c", {"b"})};
  spec.nodes[2].job.type = "";  // invalid, discovered only when that job is created (third)
  EXPECT_THROW(deps_->CreateWorkflow(spec), ValidationError);
  EXPECT_EQ(Count("jobs"), 0);
  EXPECT_EQ(Count("job_events"), 0);
}

TEST_F(DependencyServiceTest, ADatabaseFailureWhileStoringEdgesRollsBackTheWholeWorkflow) {
  Sql("CREATE FUNCTION tf_inject_failure() RETURNS trigger LANGUAGE plpgsql AS "
      "$$ BEGIN RAISE EXCEPTION 'injected failure' USING ERRCODE = 'XX999'; END $$");
  Sql("CREATE TRIGGER tf_inject BEFORE INSERT ON job_dependencies FOR EACH ROW EXECUTE FUNCTION tf_inject_failure()");

  WorkflowSpec spec;
  spec.nodes = {Node("a"), Node("b", {"a"}), Node("c", {"a", "b"})};
  EXPECT_THROW(deps_->CreateWorkflow(spec), DatabaseError);
  EXPECT_EQ(Count("jobs"), 0) << "the jobs created earlier in the transaction must be rolled back";
  EXPECT_EQ(Count("job_dependencies"), 0);
  EXPECT_EQ(Count("job_events"), 0);

  Sql("DROP TRIGGER tf_inject ON job_dependencies");
  EXPECT_NO_THROW(deps_->CreateWorkflow(spec));  // the same call works once the fault is gone
  EXPECT_EQ(Count("jobs"), 3);
  EXPECT_EQ(Count("job_dependencies"), 3);
}

TEST_F(DependencyServiceTest, ADatabaseFailureWhileAddingEdgesLeavesThePreviousGraphIntact) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{b.id, a.id}});
  Sql("CREATE FUNCTION tf_inject_failure() RETURNS trigger LANGUAGE plpgsql AS "
      "$$ BEGIN RAISE EXCEPTION 'injected failure' USING ERRCODE = 'XX999'; END $$");
  Sql("CREATE TRIGGER tf_inject BEFORE INSERT ON job_dependencies FOR EACH ROW "
      "WHEN (NEW.job_id = '" + c.id.str() + "') EXECUTE FUNCTION tf_inject_failure()");
  EXPECT_THROW(deps_->AddDependencies({{b.id, a.id}}), ConflictError);  // already stored
  EXPECT_THROW(deps_->AddDependencies({{c.id, a.id}, {c.id, b.id}}), DatabaseError);
  EXPECT_EQ(Count("job_dependencies"), 1);
}

// ---------- inspection ----------

TEST_F(DependencyServiceTest, AncestorsDescendantsAndTheTopologicalView) {
  WorkflowSpec spec;
  spec.nodes = {Node("A"), Node("B", {"A"}), Node("C", {"A"}), Node("D", {"B", "C"}), Node("E", {"D"}),
                Node("other")};
  const auto created = deps_->CreateWorkflow(spec);
  const auto id = [&](const char* key) { return created.jobs.at(key).id; };

  EXPECT_EQ(Names(deps_->Ancestors(id("D"))),
            (std::set<std::string>{id("A").str(), id("B").str(), id("C").str()}));
  EXPECT_EQ(Names(deps_->Descendants(id("B"))), (std::set<std::string>{id("D").str(), id("E").str()}));
  EXPECT_TRUE(deps_->Ancestors(id("A")).empty());
  EXPECT_TRUE(deps_->Descendants(id("other")).empty());

  const auto view = deps_->TopologicalView(id("B"));  // B, its ancestor A, its descendants D and E
  EXPECT_EQ(Names(view), (std::set<std::string>{id("A").str(), id("B").str(), id("D").str(), id("E").str()}));
  const auto position = [&](const char* key) {
    return std::find(view.begin(), view.end(), id(key)) - view.begin();
  };
  EXPECT_LT(position("A"), position("B"));
  EXPECT_LT(position("B"), position("D"));
  EXPECT_LT(position("D"), position("E"));
}

TEST_F(DependencyServiceTest, PrerequisiteCompletionIsReportedFromTheRealJobStates) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{c.id, a.id}, {c.id, b.id}});
  EXPECT_FALSE(deps_->ArePrerequisitesCompleted(c.id));
  EXPECT_EQ(Names(deps_->UnmetPrerequisites(c.id)), Names({a, b}));
  Complete(a);
  EXPECT_FALSE(deps_->ArePrerequisitesCompleted(c.id));
  EXPECT_EQ(Names(deps_->UnmetPrerequisites(c.id)), Names({b}));
  Complete(b);
  EXPECT_TRUE(deps_->ArePrerequisitesCompleted(c.id));
  EXPECT_TRUE(deps_->ArePrerequisitesCompleted(a.id));  // no prerequisites at all
  EXPECT_THROW(deps_->ArePrerequisitesCompleted(JobId::Parse("99999999-9999-9999-9999-999999999999")),
               NotFoundError);
}

// ---------- failure propagation ----------

TEST_F(DependencyServiceTest, AFailedPrerequisiteFailsEveryDependentTransitively) {
  WorkflowSpec spec;
  spec.nodes = {Node("A"), Node("B", {"A"}), Node("C", {"A"}), Node("D", {"B", "C"}), Node("E", {"D"}),
                Node("independent")};
  const auto created = deps_->CreateWorkflow(spec);
  const auto entry = [&](const char* key) { return repo_->GetSchedulingEntries({created.jobs.at(key).id}).front(); };

  Finish(entry("A"), JobState::kFailed);

  for (const char* key : {"B", "C", "D", "E"}) {
    EXPECT_EQ(State(entry(key)), JobState::kFailed) << key;
    EXPECT_TRUE(repo_->GetJob(created.jobs.at(key).id)->finished_at) << key;
  }
  EXPECT_EQ(State(entry("independent")), JobState::kPending);  // unrelated work is untouched
  // The error says which prerequisite decided it.
  EXPECT_EQ(LastError(entry("B")), "prerequisite " + created.jobs.at("A").id.str() + " failed");
  const std::string d_error = *LastError(entry("D"));
  EXPECT_TRUE(d_error == "prerequisite " + created.jobs.at("B").id.str() + " failed" ||
              d_error == "prerequisite " + created.jobs.at("C").id.str() + " failed")
      << d_error;
  EXPECT_EQ(LastError(entry("E")), "prerequisite " + created.jobs.at("D").id.str() + " failed");
}

TEST_F(DependencyServiceTest, PropagationWritesOneAuditEventPerEndedJobAndKeepsTheLogAppendOnly) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{b.id, a.id}, {c.id, b.id}});
  const auto versions_before = repo_->GetJob(b.id)->version;
  Finish(a, JobState::kFailed);

  for (const auto& job : {b, c}) {
    const auto events = repo_->GetEvents(job.id);
    ASSERT_EQ(events.size(), 2u) << "creation + exactly one propagation event";
    EXPECT_EQ(events[1].from_state, JobState::kPending);
    EXPECT_EQ(events[1].to_state, JobState::kFailed);
    ASSERT_TRUE(events[1].reason.has_value());
    EXPECT_NE(events[1].reason->find("prerequisite"), std::string::npos);
  }
  EXPECT_EQ(repo_->GetJob(b.id)->version, versions_before + 1);  // updated exactly once
  EXPECT_THROW(Sql("UPDATE job_events SET reason = 'edited'"), DatabaseError);
  EXPECT_EQ(deps_->ResolveDependents(a.id).failed.size(), 0u) << "idempotent: nothing left to end";
  EXPECT_EQ(repo_->GetEvents(b.id).size(), 2u);
}

TEST_F(DependencyServiceTest, ATimedOutPrerequisiteFailsItsDependents) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  Finish(a, JobState::kTimeout);
  EXPECT_EQ(State(b), JobState::kFailed);
  EXPECT_EQ(LastError(b), "prerequisite " + a.id.str() + " timed out");
}

TEST_F(DependencyServiceTest, ACancelledPrerequisiteCancelsItsDependents) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{b.id, a.id}, {c.id, b.id}});
  repo_->UpdateJobState(a.id, JobState::kCancelled);
  EXPECT_EQ(State(b), JobState::kCancelled);
  EXPECT_EQ(State(c), JobState::kCancelled);
  EXPECT_EQ(LastError(b), "prerequisite " + a.id.str() + " was cancelled");
}

TEST_F(DependencyServiceTest, CancellingARunningPrerequisiteAlsoCancelsDependents) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  Start(a);
  repo_->UpdateJobState(a.id, JobState::kCancelled);
  EXPECT_EQ(State(b), JobState::kCancelled);
}

TEST_F(DependencyServiceTest, TheFirstNonSuccessfulPrerequisiteDecidesHowADependentEnds) {
  const auto failed = Due(), cancelled = Due(), child = Due();
  deps_->AddDependencies({{child.id, cancelled.id}, {child.id, failed.id}});
  repo_->UpdateJobState(cancelled.id, JobState::kCancelled);
  EXPECT_EQ(State(child), JobState::kCancelled) << "only a cancelled prerequisite so far";

  const auto failed2 = Due(), cancelled2 = Due(), child2 = Due();
  deps_->AddDependencies({{child2.id, failed2.id}, {child2.id, cancelled2.id}});
  Finish(failed2, JobState::kFailed);
  EXPECT_EQ(State(child2), JobState::kFailed);
  (void)failed;  // still PENDING: the child was already ended by the cancelled prerequisite
}

TEST_F(DependencyServiceTest, ARetryableFailureDoesNotPropagateBecauseThePrerequisiteMayStillSucceed) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  const JobAttempt attempt = Start(a);
  AttemptOutcome failure;
  failure.state = AttemptState::kFailed;
  failure.error_message = "transient";
  repo_->StoreResult(a.id, attempt.attempt_number, failure, JobState::kRetryWait);
  EXPECT_EQ(State(a), JobState::kRetryWait);
  EXPECT_EQ(State(b), JobState::kPending) << "RETRY_WAIT is not final";
}

TEST_F(DependencyServiceTest, DependentsThatAreNotPendingAreLeftAlone) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  repo_->UpdateJobState(b.id, JobState::kCancelled);  // b was already ended by someone else
  Finish(a, JobState::kFailed);
  EXPECT_EQ(State(b), JobState::kCancelled);
  EXPECT_EQ(repo_->GetEvents(b.id).size(), 2u);  // creation + the manual cancel, nothing added
}

TEST_F(DependencyServiceTest, AnEdgeToAnAlreadyFailedPrerequisiteEndsTheDependentImmediately) {
  const auto a = Due(), b = Due();
  Finish(a, JobState::kFailed);
  deps_->AddDependencies({{b.id, a.id}});
  EXPECT_EQ(State(b), JobState::kFailed);
  EXPECT_EQ(Count("job_dependencies"), 1);

  const auto cancelled = Due(), c = Due();
  repo_->UpdateJobState(cancelled.id, JobState::kCancelled);
  deps_->AddDependencies({{c.id, cancelled.id}});
  EXPECT_EQ(State(c), JobState::kCancelled);
}

TEST_F(DependencyServiceTest, ARequeuedPrerequisiteNeverReleasesItsFailedDependentsEarly) {
  const auto a = Due(), b = Due();
  Edge(b, a);
  Finish(a, JobState::kFailed);
  ASSERT_EQ(State(b), JobState::kFailed);

  repo_->UpdateJobState(a.id, JobState::kReady);  // manual requeue of the prerequisite
  EXPECT_EQ(State(b), JobState::kFailed) << "the dependent stays ended";
  EXPECT_THROW(repo_->UpdateJobState(b.id, JobState::kReady), ConflictError)
      << "and cannot be forced to READY while its prerequisite has not succeeded";

  const JobAttempt attempt = repo_->CreateAttempt(a.id);
  EXPECT_THROW(repo_->UpdateJobState(b.id, JobState::kReady), ConflictError);  // still running
  repo_->StoreResult(a.id, attempt.attempt_number, AttemptOutcome{}, JobState::kCompleted);
  EXPECT_NO_THROW(repo_->UpdateJobState(b.id, JobState::kReady)) << "now every prerequisite has actually succeeded";
  EXPECT_EQ(State(b), JobState::kReady);
}

TEST_F(DependencyServiceTest, ResolveDependentsReportsWhoIsReleasedByACompletedPrerequisite) {
  const auto a = Due(), b = Due(), c = Due(), d = Due();
  deps_->AddDependencies({{c.id, a.id}, {c.id, b.id}, {d.id, a.id}});

  Complete(a);
  auto result = deps_->ResolveDependents(a.id);
  EXPECT_EQ(Names(result.released), Names({d}));  // d waits only for a; c still waits for b
  EXPECT_TRUE(result.failed.empty());
  Complete(b);
  result = deps_->ResolveDependents(b.id);
  EXPECT_EQ(Names(result.released), Names({c}));
  EXPECT_EQ(State(c), JobState::kPending) << "resolution reports; the scheduler performs the release";
}

TEST_F(DependencyServiceTest, ResolveBlockedRepairsJobsStrandedByAChangeMadeOutsideTheRepository) {
  const auto a = Due(), b = Due(), c = Due();
  deps_->AddDependencies({{b.id, a.id}, {c.id, b.id}});
  // A manual SQL update bypasses the repository, so nothing propagates.
  Sql("UPDATE jobs SET status = 'FAILED', finished_at = now() WHERE id = $1::uuid", {a.id.str()});
  ASSERT_EQ(State(b), JobState::kPending);

  EXPECT_EQ(deps_->ResolveBlocked(100), 2u);  // b and, transitively, c
  EXPECT_EQ(State(b), JobState::kFailed);
  EXPECT_EQ(State(c), JobState::kFailed);
  EXPECT_EQ(deps_->ResolveBlocked(100), 0u);  // idempotent
}

TEST_F(DependencyServiceTest, ConcurrentFailurePropagationEndsEachDependentExactlyOnce) {
  for (int round = 0; round < 10; ++round) {
    const auto a = Due(), b = Due();
    std::vector<SchedulingEntry> dependents;
    for (int i = 0; i < 20; ++i) {
      dependents.push_back(Due());
      deps_->AddDependencies({{dependents.back().id, a.id}, {dependents.back().id, b.id}});
    }
    Start(a);
    Start(b);
    AttemptOutcome failure;
    failure.state = AttemptState::kFailed;
    failure.error_message = "boom";
    Race([&] { repo_->StoreResult(a.id, 1, failure, JobState::kFailed); },
         [&] { repo_->StoreResult(b.id, 1, failure, JobState::kFailed); });
    for (const auto& job : dependents) {
      ASSERT_EQ(State(job), JobState::kFailed) << "round " << round;
      ASSERT_EQ(repo_->GetEvents(job.id).size(), 2u) << "one propagation event, not two: round " << round;
      ASSERT_EQ(repo_->GetJob(job.id)->version, 2) << "round " << round;
    }
  }
}

}  // namespace
}  // namespace taskflow
