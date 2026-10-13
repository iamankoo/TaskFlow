# Scheduler baseline (Phase 3)

An **early baseline**, measured once, not a load test. Phase 9 performs the formal benchmark with sustained load, multiple schedulers and workers.

Reproduce: `docker compose --profile test run --rm --entrypoint sh taskflow-test -c "./build/ci/taskflow_scheduler_bench 5000"` (source: `bench/scheduler_bench.cpp`). It creates a throwaway PostgreSQL schema and uses Redis database 15, so it is safe next to other data.

## Environment

- GitHub Codespace `standardLinux32gb`: 4 vCPU, 16 GB RAM, Docker 29.8 / Compose 5.5.
- PostgreSQL 16 and Redis 7 in containers on the **same VM** as the benchmark process (one network bridge, shared CPU). Network latency to both is therefore a container-bridge hop, not a real network.
- Release build (`-O3`), 5,000 jobs, 1 scheduler instance, Redis without persistence.
- Run date 2026-10-13. One run; no warm-up repetitions, so expect run-to-run variation.

## Results

| Measurement | Samples | p50 | p95 | p99 | mean | Unit |
|---|---|---|---|---|---|---|
| Heap push (in memory, 100k jobs, aging on) | 100,000 | 0.28 | 0.75 | 2.56 | 0.50 | µs |
| Heap pop, highest priority (100k jobs) | 100,000 | 1.48 | 2.18 | 2.54 | 1.54 | µs |
| Redis queue insert (`ZADD`, one job) | 5,000 | 57.00 | 77.85 | 100.69 | 59.25 | µs |
| Redis queue extract (Lua `PopNext`, aging on) | 5,000 | 89.45 | 115.68 | 136.75 | 87.35 | µs |
| Redis queue insert, batch of 500 | 20 | 804.79 | 958.35 | 1237.41 | 843.46 | µs per batch |
| Scheduler tick (500 promotions + Redis publish) | 10 | 46.04 | 101.70 | 101.70 | 51.48 | ms |
| PostgreSQL promotion, batch of 500 jobs | 10 | 36.62 | 39.21 | 39.21 | 36.55 | ms per batch |
| PostgreSQL promotion, per job | 10 | 73.23 | 78.42 | 78.42 | 73.09 | µs per job |

Throughput and recovery:

- **Scheduling throughput: 9,642 jobs/sec**: 5,000 due jobs promoted and published by repeated scheduler ticks (batches of 500) in 0.519 s wall time. Each tick selects candidates from PostgreSQL, orders them with the heap, promotes them in one transaction and publishes them to Redis.
- **Reconciliation after a total Redis loss** (`FLUSHDB`): rebuilt 10,000 ready entries in **41.9 ms**.

## Reading the numbers

- Rows with 10 samples (ticks, promotion batches) have coarse percentiles: p95 and p99 are the same sample. Treat them as indicative.
- The heap timings include the cost of reading the clock around each call (tens of nanoseconds).
- The one slow tick (101.7 ms against a typical 46 ms) is most likely the first tick, which also runs the initial reconciliation; this was not measured separately.
- The Redis timings are single-client round trips on one VM; they measure round-trip latency more than Redis itself.
- Nothing here exercises concurrent schedulers, large backlogs (> 5,000 jobs), long-running soak, or competing worker traffic. Those belong to Phase 9.
