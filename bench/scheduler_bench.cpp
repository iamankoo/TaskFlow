// Early scheduler baseline (Phase 3). Everything is measured against real PostgreSQL and
// Redis in a throwaway schema / Redis database 15; nothing here is estimated.
// Phase 9 performs the formal load benchmark.
//
//   taskflow_scheduler_bench [jobs]        (default 5000 jobs; PostgreSQL/Redis from TASKFLOW_* env)

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "taskflow/config/config.hpp"
#include "taskflow/db/job_repository.hpp"
#include "taskflow/db/migration_runner.hpp"
#include "taskflow/redis/redis_client.hpp"
#include "taskflow/scheduler/priority_queue.hpp"
#include "taskflow/scheduler/schedule_queue.hpp"
#include "taskflow/scheduler/scheduler.hpp"

using namespace taskflow;
using Clock = std::chrono::steady_clock;

namespace {

struct Stats {
  std::vector<double> samples;  // microseconds
  void Add(double us) { samples.push_back(us); }
  double Percentile(double p) const {
    if (samples.empty()) return 0;
    std::vector<double> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const auto index = static_cast<std::size_t>(p / 100.0 * static_cast<double>(sorted.size() - 1) + 0.5);
    return sorted[std::min(index, sorted.size() - 1)];
  }
  double Mean() const {
    double sum = 0;
    for (const double s : samples) sum += s;
    return samples.empty() ? 0 : sum / static_cast<double>(samples.size());
  }
};

double MicrosSince(Clock::time_point start) {
  return std::chrono::duration<double, std::micro>(Clock::now() - start).count();
}

void Row(const char* name, const Stats& s, const char* unit) {
  std::printf("| %-44s | %10.2f | %10.2f | %10.2f | %10.2f | %s |\n", name, s.Percentile(50), s.Percentile(95),
              s.Percentile(99), s.Mean(), unit);
}

SchedulingEntry MakeEntry(std::int64_t sequence, JobPriority priority, std::int64_t scheduled_us) {
  char buffer[40];
  std::snprintf(buffer, sizeof(buffer), "00000000-0000-0000-0000-%012lld", static_cast<long long>(sequence));
  SchedulingEntry entry;
  entry.id = JobId::Parse(buffer);
  entry.sequence = sequence;
  entry.priority = priority;
  entry.state = JobState::kReady;
  entry.scheduled_at = TimePoint(std::chrono::microseconds(scheduled_us));
  return entry;
}

}  // namespace

int main(int argc, char** argv) {
  const int job_count = argc > 1 ? std::atoi(argv[1]) : 5000;
  Config config;
  config.ApplyEnvironment(ProcessEnv());
  config.Validate();

  std::printf("# TaskFlow scheduler baseline\n\n");
  std::printf("jobs=%d  postgres=%s:%u  redis=%s:%u  hardware_threads=%u\n\n", job_count,
              config.postgres.host.c_str(), config.postgres.port, config.redis.host.c_str(), config.redis.port,
              std::thread::hardware_concurrency());
  std::printf("| %-44s | %10s | %10s | %10s | %10s | unit |\n", "measurement", "p50", "p95", "p99", "mean");
  std::printf("|%s|%s|%s|%s|%s|------|\n", std::string(46, '-').c_str(), std::string(12, '-').c_str(),
              std::string(12, '-').c_str(), std::string(12, '-').c_str(), std::string(12, '-').c_str());

  // ---------- A. in-memory priority queue ----------
  {
    const int n = 100'000;
    std::mt19937_64 random(1);
    std::vector<SchedulingEntry> entries;
    for (int i = 1; i <= n; ++i) {
      entries.push_back(MakeEntry(i, kPrioritiesHighestFirst[random() % 4],
                                  1'800'000'000'000'000 + static_cast<std::int64_t>(random() % 60'000'000)));
    }
    JobPriorityQueue queue(PriorityPolicy(std::chrono::seconds(60)));
    Stats push, pop;
    for (const auto& entry : entries) {
      const auto t = Clock::now();
      queue.Push(entry);
      push.Add(MicrosSince(t) );
    }
    while (!queue.empty()) {
      const auto t = Clock::now();
      queue.Pop();
      pop.Add(MicrosSince(t));
    }
    Row("heap push, 100k jobs (in memory)", push, "us");
    Row("heap pop (highest priority), 100k jobs", pop, "us");
  }

  // ---------- environment for the rest ----------
  const std::string schema = "tf_bench_" + std::to_string(::getpid());
  const PostgresConfig admin_postgres = config.postgres;  // default search_path, used for setup and cleanup
  PgConnection::Connect(admin_postgres)->Exec("CREATE SCHEMA " + schema);
  config.postgres.options = "-csearch_path=" + schema;
  config.postgres.pool_size = 4;
  config.redis.database = 15;
  PgPool pool(config.postgres);
  MigrationRunner(pool).Run(LoadMigrations(TASKFLOW_SOURCE_MIGRATIONS_DIR));
  RedisClient redis(config.redis);
  redis.Execute({"FLUSHDB"});
  const std::string prefix = "tfbench";
  ScheduleQueue queue(redis, prefix);
  JobRepository repo(pool);

  int exit_code = 0;
  try {
    // ---------- B. Redis queue operations ----------
    {
      const int n = std::min(job_count, 5000);
      std::mt19937_64 random(2);
      std::vector<SchedulingEntry> entries;
      for (int i = 1; i <= n; ++i) {
        entries.push_back(MakeEntry(i, kPrioritiesHighestFirst[random() % 4],
                                    1'800'000'000'000'000 + static_cast<std::int64_t>(random() % 60'000'000)));
      }
      Stats insert, extract;
      for (const auto& entry : entries) {
        const auto t = Clock::now();
        queue.AddReady({entry});
        insert.Add(MicrosSince(t));
      }
      Row("Redis queue insert (ZADD, one job)", insert, "us");
      for (int i = 0; i < n; ++i) {
        const auto t = Clock::now();
        const auto popped = queue.PopNext(std::chrono::seconds(60));
        extract.Add(MicrosSince(t));
        if (!popped) break;
      }
      Row("Redis queue extract (Lua PopNext, aging on)", extract, "us");

      Stats batch;
      for (int round = 0; round < 20; ++round) {
        std::vector<SchedulingEntry> chunk(entries.begin(), entries.begin() + std::min<int>(500, n));
        const auto t = Clock::now();
        queue.AddReady(chunk);
        batch.Add(MicrosSince(t));
      }
      Row("Redis queue insert, batch of 500", batch, "us/batch");
      redis.Execute({"FLUSHDB"});
    }

    // ---------- C. PostgreSQL promotion + scheduler ticks ----------
    {
      const auto hour_ago = std::chrono::time_point_cast<std::chrono::seconds>(
          std::chrono::system_clock::now() - std::chrono::hours(1));
      auto now = hour_ago + std::chrono::seconds(10);
      auto lease = pool.Acquire();
      lease->Query(
          "INSERT INTO jobs (type, priority, status, scheduled_at) "
          "SELECT 'bench.job', i % 4, 'PENDING', "
          "       to_timestamp($1::bigint) + (i % 5000) * interval '1 millisecond' "
          "FROM generate_series(1, $2::int) i",
          {std::to_string(std::chrono::duration_cast<std::chrono::seconds>(hour_ago.time_since_epoch()).count()),
           std::to_string(job_count)});
      lease = {};

      SchedulerConfig sc;
      sc.tick_interval_ms = 10;
      sc.max_promotions_per_tick = 500;
      sc.candidates_per_priority = 500;
      sc.aging_interval_ms = 60'000;
      sc.reconcile_interval_ticks = 1'000'000;  // reconcile only on the first tick; measured separately below
      sc.redis_prefix = prefix;
      Scheduler scheduler(repo, queue, sc, [&] { return now; }, "bench");

      Stats tick;
      std::size_t promoted_total = 0;
      const auto wall_start = Clock::now();
      for (;;) {
        const auto result = scheduler.Tick();
        tick.Add(static_cast<double>(result.duration.count()) / 1000.0);  // ms
        promoted_total += result.promoted;
        if (result.promoted == 0) break;
      }
      const double wall_seconds = MicrosSince(wall_start) / 1e6;
      tick.samples.pop_back();  // the final empty tick is not representative of a loaded tick
      Row("scheduler tick (500 promotions + publish)", tick, "ms");
      std::printf("\n- scheduler promoted %zu jobs in %.3f s wall time => **%.0f jobs/sec** "
                  "(PostgreSQL promotion + Redis publish, batches of 500)\n",
                  promoted_total, wall_seconds, static_cast<double>(promoted_total) / wall_seconds);

      // promotion latency per batch, isolated from candidate selection and Redis
      {
        const auto more = job_count;
        auto l2 = pool.Acquire();
        l2->Query(
            "INSERT INTO jobs (type, priority, status, scheduled_at) "
            "SELECT 'bench.job', i % 4, 'PENDING', to_timestamp($1::bigint) "
            "FROM generate_series(1, $2::int) i",
            {std::to_string(std::chrono::duration_cast<std::chrono::seconds>(hour_ago.time_since_epoch()).count()),
             std::to_string(more)});
        l2 = {};
        Stats promote_batch, promote_job;
        const auto due = repo.FindDueJobs(now, 100000);
        for (std::size_t begin = 0; begin + 500 <= due.size(); begin += 500) {
          std::vector<JobId> ids;
          for (std::size_t i = begin; i < begin + 500; ++i) ids.push_back(due[i].id);
          const auto t = Clock::now();
          const auto promoted = repo.PromoteJobsToReady(ids, now);
          const double us = MicrosSince(t);
          promote_batch.Add(us / 1000.0);
          promote_job.Add(us / static_cast<double>(std::max<std::size_t>(promoted.size(), 1)));
        }
        Row("PostgreSQL promotion, batch of 500 jobs", promote_batch, "ms/batch");
        Row("PostgreSQL promotion, per job", promote_job, "us/job");
      }

      // reconciliation after a total Redis loss
      redis.Execute({"FLUSHDB"});
      const auto t = Clock::now();
      const auto report = scheduler.Reconcile();
      const double reconcile_ms = MicrosSince(t) / 1000.0;
      std::printf("- reconciliation after FLUSHDB: rebuilt %zu ready + %zu delayed entries in %.1f ms\n",
                  report.ready_added, report.delayed_added, reconcile_ms);
    }
  } catch (const std::exception& e) {
    std::fprintf(stderr, "benchmark failed: %s\n", e.what());
    exit_code = 1;
  }

  try {
    redis.Execute({"FLUSHDB"});
    PgConnection::Connect(admin_postgres)->Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
  } catch (...) {
  }
  return exit_code;
}
