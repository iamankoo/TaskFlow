// DAG baseline (Phase 4). Graph algorithms are measured in memory; everything that touches the
// database runs against real PostgreSQL and Redis in a throwaway schema / Redis database 15.
// Nothing here is estimated. Phase 9 performs the formal load benchmark.
//
//   taskflow_dag_bench        (PostgreSQL / Redis from TASKFLOW_* environment)

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "taskflow/config/config.hpp"
#include "taskflow/dag/dependency_graph.hpp"
#include "taskflow/dag/dependency_service.hpp"
#include "taskflow/db/job_repository.hpp"
#include "taskflow/db/migration_runner.hpp"
#include "taskflow/redis/redis_client.hpp"
#include "taskflow/scheduler/schedule_queue.hpp"
#include "taskflow/scheduler/scheduler.hpp"

using namespace taskflow;
using Clock = std::chrono::steady_clock;

namespace {

double MillisSince(Clock::time_point start) {
  return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

JobId Id(std::int64_t n) {
  char buffer[40];
  std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0000-%012lld", static_cast<long long>(n));
  return JobId::Parse(buffer);
}

struct Shape {
  std::string name;
  std::function<void(DependencyGraph&)> build;
};

double Median(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  return values[values.size() / 2];
}

double Percentile(std::vector<double> values, double p) {
  std::sort(values.begin(), values.end());
  const auto index = static_cast<std::size_t>(p / 100.0 * static_cast<double>(values.size() - 1) + 0.5);
  return values[std::min(index, values.size() - 1)];
}

}  // namespace

int main() {
  Config config;
  config.ApplyEnvironment(ProcessEnv());
  config.Validate();
  std::printf("# TaskFlow DAG baseline\n\npostgres=%s:%u redis=%s:%u hardware_threads=%u\n\n",
              config.postgres.host.c_str(), config.postgres.port, config.redis.host.c_str(), config.redis.port,
              std::thread::hardware_concurrency());

  // ---------- A. in-memory graph algorithms ----------
  std::printf("## In-memory validation (median of 5 runs)\n\n");
  std::printf("| shape | nodes | edges | build ms | cycle check ms | topological sort ms | sort ns/edge |\n");
  std::printf("|---|---|---|---|---|---|---|\n");
  const std::vector<Shape> shapes = {
      {"linear chain", [](DependencyGraph& g) { for (std::int64_t i = 2; i <= 100'000; ++i) g.AddEdge(Id(i), Id(i - 1)); }},
      {"wide fan-out + fan-in", [](DependencyGraph& g) {
         for (std::int64_t i = 2; i <= 50'001; ++i) { g.AddEdge(Id(i), Id(1)); g.AddEdge(Id(50'002), Id(i)); }
       }},
      {"layered 100x1000, 3 prerequisites", [](DependencyGraph& g) {
         for (std::int64_t layer = 1; layer < 100; ++layer)
           for (std::int64_t i = 0; i < 1000; ++i)
             for (const std::int64_t k : {0, 1, 7})
               g.AddEdge(Id(layer * 1000 + i + 1), Id((layer - 1) * 1000 + (i + k) % 1000 + 1));
       }},
  };
  for (const auto& shape : shapes) {
    std::vector<double> build, cycle, sort;
    std::size_t nodes = 0, edges = 0;
    for (int run = 0; run < 5; ++run) {
      DependencyGraph graph;
      auto t = Clock::now();
      shape.build(graph);
      build.push_back(MillisSince(t));
      t = Clock::now();
      const bool cyclic = graph.FindCycle().has_value();
      cycle.push_back(MillisSince(t));
      t = Clock::now();
      const auto order = graph.TopologicalOrder();
      sort.push_back(MillisSince(t));
      nodes = graph.node_count();
      edges = graph.edge_count();
      if (cyclic || order.size() != nodes) { std::fprintf(stderr, "unexpected result for %s\n", shape.name.c_str()); return 1; }
    }
    std::printf("| %s | %zu | %zu | %.1f | %.1f | %.1f | %.0f |\n", shape.name.c_str(), nodes, edges, Median(build),
                Median(cycle), Median(sort), Median(sort) * 1e6 / static_cast<double>(edges));
  }

  // ---------- environment for the database parts ----------
  const std::string schema = "tf_dagbench_" + std::to_string(::getpid());
  const PostgresConfig admin = config.postgres;
  PgConnection::Connect(admin)->Exec("CREATE SCHEMA " + schema);
  config.postgres.options = "-csearch_path=" + schema;
  config.postgres.pool_size = 4;
  config.redis.database = 15;
  PgPool pool(config.postgres);
  MigrationRunner(pool).Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
  RedisClient redis(config.redis);
  redis.Execute({"FLUSHDB"});
  JobRepository repo(pool);
  DependencyService deps(pool, repo);
  ScheduleQueue queue(redis, "tfdagbench");

  int exit_code = 0;
  try {
    std::printf("\n## PostgreSQL operations\n\n| measurement | value |\n|---|---|\n");

    // Dependency insertion: a workflow of 20 layers x 50 jobs, 2 prerequisites each.
    {
      WorkflowSpec spec;
      for (int layer = 0; layer < 20; ++layer) {
        for (int i = 0; i < 50; ++i) {
          WorkflowNode node;
          node.key = std::to_string(layer) + "_" + std::to_string(i);
          node.job.type = "bench.workflow";
          if (layer > 0) {
            node.depends_on = {std::to_string(layer - 1) + "_" + std::to_string(i),
                               std::to_string(layer - 1) + "_" + std::to_string((i + 1) % 50)};
          }
          spec.nodes.push_back(std::move(node));
        }
      }
      const auto t = Clock::now();
      const auto created = deps.CreateWorkflow(spec);
      const double ms = MillisSince(t);
      const std::size_t edges = 2 * 50 * 19;
      std::printf("| CreateWorkflow: %zu jobs + %zu edges, one transaction | %.1f ms (%.0f jobs/s, %.0f edges/s) |\n",
                  created.jobs.size(), edges, ms, static_cast<double>(created.jobs.size()) * 1000 / ms,
                  static_cast<double>(edges) * 1000 / ms);
    }

    // AddDependencies on a growing chain: each call re-validates the whole stored ancestry.
    {
      auto lease = pool.Acquire();
      lease->Query("INSERT INTO jobs (type) SELECT 'bench.chain' FROM generate_series(1, 2001)");
      std::vector<JobId> chain;
      const auto rows = lease->Query("SELECT id::text FROM jobs WHERE type = 'bench.chain' ORDER BY sequence");
      for (std::size_t i = 0; i < rows.rows.size(); ++i) chain.push_back(JobId::Parse(rows.Value(i, 0)));
      lease = {};
      std::vector<double> latency;
      for (std::size_t i = 1; i < chain.size(); ++i) {
        const auto t = Clock::now();
        deps.AddDependencies({{chain[i], chain[i - 1]}});
        latency.push_back(MillisSince(t));
      }
      std::printf("| AddDependencies, one edge onto a chain growing to 2,000 deep | p50 %.2f ms, p95 %.2f ms, p99 %.2f ms (%zu calls) |\n",
                  Median(latency), Percentile(latency, 95), Percentile(latency, 99), latency.size());
      std::printf("| first 100 calls / last 100 calls (median) | %.2f ms / %.2f ms |\n",
                  Median(std::vector<double>(latency.begin(), latency.begin() + 100)),
                  Median(std::vector<double>(latency.end() - 100, latency.end())));

      std::vector<double> validate;
      for (int i = 0; i < 20; ++i) {
        const auto t = Clock::now();
        const GraphCheck check = deps.ValidateEdges({{chain.front(), chain.back()}});  // would close a 2,001-job cycle
        validate.push_back(MillisSince(t));
        if (check.valid) throw std::runtime_error("the cycle was not detected");
      }
      std::printf("| ValidateEdges detecting a cycle through a 2,001-job chain | median %.2f ms |\n", Median(validate));
    }

    // Dependency resolution latency: a failing prerequisite ends its dependents in the same transaction.
    {
      const auto run_failure = [&](const char* label, int dependents, bool chain) {
        NewJob job;
        job.type = "bench.parent";
        job.initial_state = JobState::kReady;
        const Job parent = repo.CreateJob(job);
        std::vector<DependencyEdge> edges;
        JobId previous = parent.id;
        for (int i = 0; i < dependents; ++i) {
          NewJob child;
          child.type = "bench.child";
          const Job created = repo.CreateJob(child);
          edges.push_back({created.id, chain ? previous : parent.id});
          previous = created.id;
        }
        deps.AddDependencies(edges);
        const JobAttempt attempt = repo.CreateAttempt(parent.id);
        AttemptOutcome failure;
        failure.state = AttemptState::kFailed;
        failure.error_message = "bench";
        const auto t = Clock::now();
        repo.StoreResult(parent.id, attempt.attempt_number, failure, JobState::kFailed);
        const double ms = MillisSince(t);
        std::printf("| failure propagation, %s of %d dependents (StoreResult incl. propagation) | %.1f ms (%.0f jobs/s) |\n",
                    label, dependents, ms, dependents * 1000.0 / ms);
      };
      run_failure("fan-out", 1000, false);
      run_failure("chain", 500, true);
    }

    // Readiness propagation through the scheduler: 10 layers x 200 jobs, 2 prerequisites each.
    {
      {  // start from an empty schema so the scheduler only sees this workflow
        auto lease = pool.Acquire();
        lease->Query("DELETE FROM jobs");
      }
      redis.Execute({"FLUSHDB"});
      WorkflowSpec spec;
      constexpr int kLayers = 10, kWidth = 200;
      for (int layer = 0; layer < kLayers; ++layer) {
        for (int i = 0; i < kWidth; ++i) {
          WorkflowNode node;
          node.key = std::to_string(layer) + "_" + std::to_string(i);
          node.job.type = "bench.layered";
          if (layer > 0) {
            node.depends_on = {std::to_string(layer - 1) + "_" + std::to_string(i),
                               std::to_string(layer - 1) + "_" + std::to_string((i + 3) % kWidth)};
          }
          spec.nodes.push_back(std::move(node));
        }
      }
      const auto created = deps.CreateWorkflow(spec);
      SchedulerConfig sc;
      sc.max_promotions_per_tick = 500;
      sc.candidates_per_priority = 500;
      sc.reconcile_interval_ticks = 1'000'000;
      sc.redis_prefix = "tfdagbench";
      Scheduler scheduler(repo, queue, sc);
      std::this_thread::sleep_for(std::chrono::milliseconds(20));  // make every job's scheduled_at strictly past

      std::size_t released = 0;
      double tick_ms = 0;
      std::vector<double> per_layer;
      for (int layer = 0; layer < kLayers; ++layer) {
        const auto layer_start = Clock::now();
        std::size_t this_layer = 0;
        while (this_layer < static_cast<std::size_t>(kWidth)) {
          const auto t = Clock::now();
          const auto result = scheduler.Tick();
          tick_ms += MillisSince(t);
          if (result.promoted == 0) throw std::runtime_error("layer " + std::to_string(layer) + " was not released");
          this_layer += result.promoted;
        }
        released += this_layer;
        per_layer.push_back(MillisSince(layer_start));
        // The "workers" finish the layer (direct SQL, bench only), which unblocks the next one.
        std::string ids = "{";
        for (int i = 0; i < kWidth; ++i) {
          ids += (i ? "," : "") + created.jobs.at(std::to_string(layer) + "_" + std::to_string(i)).id.str();
        }
        ids += "}";
        auto lease = pool.Acquire();
        lease->Query("UPDATE jobs SET status = 'RUNNING', attempt_count = 1 WHERE id = ANY($1::uuid[])", {ids});
        lease->Query("UPDATE jobs SET status = 'COMPLETED', finished_at = clock_timestamp() WHERE id = ANY($1::uuid[])", {ids});
      }
      std::printf("| readiness propagation through the scheduler: %d layers x %d jobs, 2 prerequisites each | %zu jobs released, "
                  "%.1f ms in ticks => %.0f jobs/s |\n",
                  kLayers, kWidth, released, tick_ms, static_cast<double>(released) * 1000 / tick_ms);
      std::printf("| per-layer release (find due + promote + publish), median | %.1f ms |\n", Median(per_layer));
    }
  } catch (const std::exception& e) {
    std::fprintf(stderr, "benchmark failed: %s\n", e.what());
    exit_code = 1;
  }

  try {
    redis.Execute({"FLUSHDB"});
    PgConnection::Connect(admin)->Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
  } catch (...) {
  }
  return exit_code;
}
