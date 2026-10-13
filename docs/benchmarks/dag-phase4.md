# DAG baseline (Phase 4)

An **early baseline**, measured on one VM, not a load test. Phase 9 performs the formal benchmark.

Reproduce: `docker compose --profile test run --rm --entrypoint sh taskflow-test -c "./build/ci/taskflow_dag_bench"` (source: `bench/dag_bench.cpp`). Database sections run in a throwaway PostgreSQL schema and Redis database 15.

## Environment

- GitHub Codespace `standardLinux32gb`: 4 vCPU, 16 GB RAM, Docker 29.8 / Compose 5.5.
- PostgreSQL 16 and Redis 7 in containers on the **same VM** as the benchmark (one bridge network, shared CPU, shared disk).
- Release build (`-O3`), one client connection at a time (the pool is 4), Redis without persistence.
- Run date 2026-10-13. One run of each measurement (the scheduler benchmark was repeated 6 times, see below).

## Graph algorithms, in memory (median of 5 runs)

| Shape | Nodes | Edges | Build | Cycle check | Topological sort | Sort cost |
|---|---|---|---|---|---|---|
| linear chain | 100,000 | 99,999 | 69.5 ms | 0.9 ms | 3.2 ms | 32 ns/edge |
| wide: 1 root, 50,000 middle, 1 sink | 50,002 | 100,000 | 49.8 ms | 0.4 ms | 1.6 ms | 16 ns/edge |
| layered 100 × 1,000, 3 prerequisites each | 100,000 | 297,000 | 170.5 ms | 2.6 ms | 6.1 ms | 21 ns/edge |

`FindCycle` (iterative depth-first search) and `TopologicalOrder` (Kahn's algorithm) do O(V + E) work: the cost per edge stays in the same 16–32 ns band across shapes with 100,000 to 297,000 edges. A 200,000-job chain is also covered by a unit test, which would overflow the stack if any traversal were recursive.

## PostgreSQL operations

| Measurement | Result |
|---|---|
| `CreateWorkflow`: 1,000 jobs + 1,900 edges, one transaction | 958 ms (1,044 jobs/s, 1,983 edges/s) |
| `AddDependencies`: one edge onto a chain growing to 2,000 deep (2,000 calls) | p50 8.3 ms, p95 136 ms, p99 154 ms |
| same, median of the first 100 calls / the last 100 calls | 9.8 ms / 8.7 ms |
| `ValidateEdges` detecting a cycle through a 2,001-job chain | median 8.0 ms |
| failure propagation, 1,000 dependents of one job (`StoreResult` including propagation) | 94 ms (10,633 jobs/s) |
| failure propagation, chain of 500 dependents (`StoreResult` including propagation) | 675 ms (740 jobs/s) |
| readiness propagation through the scheduler, 10 layers × 200 jobs, 2 prerequisites each | 2,000 jobs released in 1,389 ms of ticks (1,440 jobs/s); median 135 ms per layer |

Observations, with the limits of what was measured:

- **Insertion cost is dominated by per-statement round trips.** `CreateWorkflow` issues two statements per job (job + audit event) plus one multi-row edge insert; it is not bulk-optimized.
- **The cycle check does not grow noticeably with chain depth** in this range (first 100 calls 9.8 ms, last 100 calls 8.7 ms, validation through a 2,001-deep chain 8 ms): each call is dominated by fixed transaction costs (advisory lock, row locks, commit), not by loading the ancestors.
- **`AddDependencies` has a bimodal latency**: about 5% of calls take roughly 130–200 ms while the median is 8 ms. This reproduced across three runs (p95 136–192 ms). The cause was not investigated; candidate explanations are autovacuum/checkpoint activity or fsync stalls on the shared Codespace disk. It is a known unknown, not a measured property of the design.
- **Propagation down a chain is slower per job than across a fan-out** (740 vs 10,633 jobs/s) because it proceeds level by level (3 statements per level, 500 levels), while a fan-out ends all dependents in one level.
- **The readiness-propagation throughput varied between runs** (1,440 to 1,902 jobs/s across three runs), which shows the noise of a shared VM.
- Dependency submission is serialized by one advisory lock by design (cycle safety); these numbers are for a single submitter, so they are an upper bound for one transaction at a time, not a multi-client throughput.

## Effect on the Phase 3 scheduler (dependency filter)

The scheduler's due-job and promotion queries now check prerequisites. The Phase 3 benchmark was repeated six times with the final query:

| | jobs/sec, 5,000 jobs, batches of 500 |
|---|---|
| Phase 3 (one run, 2026-10-13) | 9,642 |
| Phase 4, six runs | 8,272 / 8,306 / 8,439 / 8,446 / 8,623 / 8,750 |

So the scheduler is about 10–15% slower for the same workload. A direct `EXPLAIN ANALYZE` comparison on 5,000 candidate rows attributes it to the per-candidate prerequisite check: 6 ms without the filter against 19–32 ms with it (about 3–5 µs per candidate row), independent of table size and of whether `job_dependencies` is analyzed. One of the six runs contained a single 266 ms promotion batch, an outlier from the shared VM.

An earlier draft of the filter used a join inside the subquery. On a database where `job_dependencies` had no statistics, PostgreSQL could plan it as a hash join over the whole `jobs` table (one 7.7 s reading was observed and did not reproduce: the same query then took 24 ms three times). The shipped form avoids any join the planner could misplan; see `docs/DATABASE.md` ("Readiness check").

## Not measured

Concurrent submitters (the advisory lock serializes them), graphs larger than ~100,000 nodes in the database, graphs with very high fan-in in the database (the per-job limit is 1,000), long-running soak, and behaviour under competing worker traffic. Phase 9 covers these.
