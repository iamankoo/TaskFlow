# TaskFlow — Living Project Summary

> Updated after **every** completed phase. Only implemented **and validated** work is recorded as done. Authority order: `Phases.md` → `ARCHITECTURE.md` → `TECHSTACK.md` → `summary.md`.

## Project Overview

TaskFlow is a distributed job execution platform in C++20: REST API, priority scheduler, delayed jobs, DAG dependencies, concurrent worker pool, leasing/heartbeat-based failure recovery, retries, timeouts, and idempotency. PostgreSQL is the durable source of truth; Redis provides fast coordination. Repository: https://github.com/iamankoo/TaskFlow

## Current Status

**Phases 1–4 complete.** TaskFlow persists jobs (Phase 2), schedules them by priority and time with Redis queues rebuilt from PostgreSQL (Phase 3), and now supports **dependency graphs (DAGs)**: jobs can wait for other jobs, cycles are rejected even under concurrent submission, a job is released exactly once and only when every prerequisite has `COMPLETED`, and a failed or cancelled prerequisite ends its dependents instead of leaving them stuck. 340 tests pass (also under ASan + UBSan, and the concurrency tests under ThreadSanitizer) against real PostgreSQL and Redis; the Compose stack was built and validated live in GitHub Codespaces.

Nothing executes jobs yet and there is no job API: the running server exposes only `GET /health`.

## Current Phase

None in progress. Phase 5 starts only on the owner's instruction.

## Completed Phases

- **Phase 1 — Foundation & Project Infrastructure.** Commit `75e2548`.
- **Phase 2 — Job Model & Persistent State.** Commit `c523e2e`.
- **Phase 3 — Scheduler & Priority Queue.** Commit `6227a7a` (`6227a7a35004ca5746e5f35e16e7f9b48ab7d3e7`).
- **Phase 4 — DAG Dependency Engine.** Commit hash: reported in the Phase 4 completion report and recorded here by the first commit of Phase 5 (a commit cannot contain its own hash).

## Phase-by-Phase Summary

| # | Phase | Status | Commit |
|---|-------|--------|--------|
| 1 | Foundation & Project Infrastructure | **COMPLETE** | `75e2548` |
| 2 | Job Model & Persistent State | **COMPLETE** | `c523e2e` |
| 3 | Scheduler & Priority Queue | **COMPLETE** | `6227a7a` |
| 4 | DAG Dependency Engine | **COMPLETE** | see Git Commit History |
| 5 | Concurrent Worker Pool | NOT STARTED | — |
| 6 | Job Leasing, Heartbeats & Failure Recovery | NOT STARTED | — |
| 7 | Retries, Timeouts & Idempotency | NOT STARTED | — |
| 8 | REST API & Production Operations | NOT STARTED | — |
| 9 | Testing, Load Testing & Performance Engineering | NOT STARTED | — |
| 10 | Final Hardening, Benchmark & Release (FINAL) | NOT STARTED | — |

### Phases 1–3 (summary)

- **Phase 1:** build system (CMake presets, vcpkg manifest), configuration, logging, error types, PostgreSQL and Redis pools, migration runner, Drogon server with `GET /health`, lifecycle, Docker/Compose, GitHub Actions CI.
- **Phase 2:** job domain model, the eight-state machine (enforced in C++ and by a PostgreSQL trigger, with a parity test), schema `jobs`/`job_attempts`/`job_results`/`job_events`, transactional `JobRepository`, idempotency-key storage. See `docs/DATABASE.md`.
- **Phase 3:** scheduler (heap-ordered promotion, delayed jobs, aging), Redis ready/delayed sorted sets with an atomic Lua dequeue, PostgreSQL→Redis reconciliation, multi-scheduler safety via `FOR UPDATE SKIP LOCKED`. See `docs/SCHEDULER.md`.
- CI on GitHub was green on all three jobs for the Phase 1, 2 and 3 commits.

### Phase 4 detail

**Objective.** A durable, dependency-aware DAG: a job with prerequisites does not become `READY` until all of them succeed; independent jobs stay independently schedulable; cycles are rejected; dependencies survive restarts; resolution is safe under concurrent submission and several schedulers; failed prerequisites have a defined outcome.

**Dependency schema (migration `0005_create_job_dependencies.sql`).** `job_dependencies (job_id, depends_on_id, created_at)`: primary key `(job_id, depends_on_id)` (rejects duplicate edges and serves "prerequisites of a job"), foreign keys to `jobs` (`ON DELETE CASCADE` on the dependent; the prerequisite key is `DEFERRABLE INITIALLY DEFERRED` so a prerequisite cannot be deleted from under its dependents but a whole workflow can be deleted at once), `CHECK (job_id <> depends_on_id)`, `idx_job_dependencies_prerequisite (depends_on_id, job_id)` for dependents. Trigger `trg_job_dependencies_guard`: edges can only be added to a `PENDING` job and are immutable. The migration also **replaces** the Phase 2 trigger function `taskflow_guard_job_update()` (migrations 0001–0004 are untouched): it adds the `PENDING→FAILED` transition and refuses `READY` while any prerequisite is not `COMPLETED`. It adds one table and replaces one function, changing no existing row, so a Phase 3 database upgrades in place (tested with data present) and a fresh database reaches the same schema in one run. Forward-only; undo is documented in `docs/DATABASE.md`.

**State-machine change.** `PENDING → FAILED` added (now 14 legal transitions), in the C++ table (`job_state.cpp`), the trigger (0005) and the tests (the C++/database parity test covers all 56 pairs, and the explicit expected-transition table gained the edge). No other transition changed.

**Graph algorithms** (`DependencyGraph`, pure domain code, no I/O): iterative depth-first cycle search returning the cycle path, Kahn's algorithm for topological order, breadth-first ancestors/descendants, all with explicit stacks/queues (a 200,000-job chain is tested). **O(V + E)**: cycle check and topological sort do work linear in nodes plus edges (hash-map lookups counted as O(1) expected; order is deterministic by insertion). Measured: 16–32 ns per edge across 100,000 to 297,000 edges. Limits: 1,000 prerequisites per job, 100,000 edges per request, 10,000 jobs per workflow, 500,000 stored edges examined per cycle check.

**Application/domain interfaces** (`DependencyService`, SQL in `db/dependency_sql`, none in HTTP): `AddDependencies`, `CreateWorkflow` (jobs + edges atomically), `ValidateEdges`, `Prerequisites`, `Dependents`, `Ancestors`, `Descendants`, `TopologicalView`, `ArePrerequisitesCompleted`, `UnmetPrerequisites`, `ResolveDependents`, `ResolveBlocked`. No REST API (Phase 8).

**Readiness semantics.** A job with prerequisites stays `PENDING`; it is promotable when every prerequisite is `COMPLETED` **and** its own `scheduled_at` has passed. Readiness is **derived from the prerequisites' real states, not from a stored counter** (a counter can drift after a manual requeue; because `COMPLETED` is permanent the derived answer only ever goes from no to yes). The scheduler's due-job and promotion queries exclude blocked jobs (so blocked jobs cannot crowd out runnable ones) and the promotion re-checks under the `SKIP LOCKED` row lock, which makes the release exactly-once however many parents finish together and however many schedulers run; the database trigger refuses any other route to `READY` (operator, stale Redis entry, future code). The priority queue, aging and Redis design are unchanged.

**Failure propagation (policy).** In the same transaction as the transition, every `PENDING` job that transitively waits for a job that ended `FAILED` or `TIMEOUT` ends `FAILED`; one that waits for a `CANCELLED` job ends `CANCELLED`. One audit event per ended job (`prerequisite <id> failed|timed out|was cancelled`). A retryable failure (`RETRY_WAIT`) does not propagate. Adding an edge to an already ended prerequisite ends the new dependent immediately. Requeuing a failed prerequisite never revives its ended dependents, and they cannot be requeued until every prerequisite has actually completed. A periodic safety net in the scheduler (`ResolveBlocked`) ends jobs stranded by changes made outside the repository.

**Concurrency strategy.** Every transaction that adds edges starts with `pg_advisory_xact_lock` (transaction-scoped: works across all application instances, auto-released at COMMIT/ROLLBACK/crash), then locks the involved jobs `FOR UPDATE` in ascending id order, loads the stored ancestors of the new prerequisites (recursive CTE), checks `stored ∪ new` for a cycle, and inserts. Opposite edges `X→Y` / `Y→X` are therefore strictly ordered and the second is refused. This serializes graph writes only (reads, scheduling and execution never take the lock). Isolation is READ COMMITTED; correctness comes from the lock, not from SERIALIZABLE. A failing prerequisite racing a submission is resolved by the row lock (whichever commits first, the dependent ends up `FAILED`, never stranded). Deadlocks (`40P01`) and serialization failures (`40001`) roll back the transaction and are retried up to 5 times with randomized backoff. Documented in `docs/DATABASE.md` ("Concurrency").

## Implemented Features

- Everything from Phases 1–3.
- DAG validation (cycles, topological order, ancestors/descendants), cycle-safe atomic edge and workflow creation, derived dependency readiness enforced in the scheduler and in the database, transitive failure/cancellation propagation, dependency safety net, dependency inspection queries, DAG benchmark tool.

## Architecture Summary

As in `ARCHITECTURE.md` (sections 4–9 describe lifecycle, state machine, scheduler and DAG execution as implemented). Implemented: infrastructure (1), persistent job layer (2), scheduler and queues (3), dependency engine (4). Not yet: retry manager, lease manager, worker manager, worker pool.

## Technology Stack

Locked (see `TECHSTACK.md`): C++20, CMake, Drogon, PostgreSQL, Redis, Docker, Docker Compose, GoogleTest, spdlog, JSON, Prometheus-compatible metrics, GitHub Actions. Package manager: vcpkg (manifest mode, baseline `4b308f5`). No new dependencies in Phase 4; no `TECHSTACK.md` change.

## Database Summary

Tables: `schema_migrations`, `jobs`, `job_attempts`, `job_results`, `job_events`, **`job_dependencies`** (5 migrations). Dependency schema, indexes, triggers, concurrency and migration/rollback notes: `docs/DATABASE.md` ("Dependencies"). `workers` arrives in Phase 5.

## API Summary

| Endpoint | Behavior |
|---|---|
| `GET /health` | Unchanged: `200` with dependency status, or `503` `degraded`/`shutting_down`. |

No job or dependency endpoints yet (Phase 8).

## Testing Summary

**340 tests, all passing**, against real PostgreSQL 16 and Redis 7 (no mocks of either):

| Suite | Total | New in Phase 4 | Phase 4 coverage |
|---|---|---|---|
| `taskflow_unit_tests` | 118 | 19 | graph: empty, single node, chain, the diamond from the specification, multiple parents, independent components, self-dependency, duplicate edges, unknown nodes, direct/indirect/second-component cycles with path validation, deterministic order, 200,000-job chain (no stack overflow), wide and layered graphs, 100,000-edge validation |
| `taskflow_integration_tests` | 214 | 62 | schema (FKs, self/duplicate edges, guard triggers, deletion rules, the database refusing `READY` while a prerequisite is unfinished, index use), **upgrade from a Phase 3 database with data**, fresh-database migration, trigger parity (now 14 edges, 56 pairs), service (persistence, atomic multi-edge requests, direct/indirect/in-request cycles, validation without storing, workflows and their rollback incl. injected database failure), concurrency (40 rounds of opposite edges: exactly one succeeds; 8-job ring closed by 8 threads; prerequisite failing during submission; two parents failing together), propagation (chains, fan-out, timeout, cancel, running prerequisite, retryable failure, already-ended prerequisite, manual requeue, audit events, safety net), scheduler integration (blocked/released/multi-parent/diamond order/independent chains/failed prerequisite/candidate window/Redis loss and forged Redis entries/5 schedulers on a 60-way fan-out/two parents finishing together/delayed dependents), query-plan shape |
| `taskflow_http_tests` | 8 | 0 | unchanged |

Results (all in GitHub Codespaces):

- Release build with `-Werror` (`ci` preset): **340/340 pass**, 0 skipped.
- ASan + UBSan: **340/340 pass**, 0 skipped, no sanitizer reports.
- ThreadSanitizer, all concurrency-related tests (scheduler, dependency service, DAG scheduler, queues, pool: 145 tests): **all pass**, no data-race reports. The remaining suites were not run under TSan.
- CI on GitHub: Phases 1, 2 and 3 green on all three jobs. **The Phase 4 CI run had not finished when this was written.**
- Bugs found and fixed by validation along the way: a migration foreign key whose `NO ACTION` check broke deleting a whole workflow in one statement (now deferred); a dependency filter whose join the planner could turn into a whole-table scan when `job_dependencies` had no statistics (rewritten, plan shape now tested); test mistakes.

**Docker/Codespaces validation (live stack: Codespace `standardLinux32gb`, 4 vCPU, 16 GB, Docker 29.8, scheduler tick 200 ms):**

- PostgreSQL, Redis and TaskFlow built and became healthy; log shows `migrations complete: 5 applied`; `/health` `200`; `schema_migrations` lists version 5 `create_job_dependencies`; `job_dependencies` has `pk_job_dependencies` and `idx_job_dependencies_prerequisite`.
- Diamond workflow A → (B, C) → D → E plus an independent job X, all due: only A and X were released; B–E stayed `PENDING` although due. The database refused a manual `UPDATE … SET status='READY'` on a blocked job (`cannot become READY: it has prerequisites that are not COMPLETED`). Completing A released B and C together; completing B alone left D blocked; completing C released D; completing D released E. Release order from `job_events`: A > X > B > C > D > E, each job released exactly once.
- A failed prerequisite (changed by raw SQL, bypassing the repository): its dependents G, H and I were ended `PENDING→FAILED` by the scheduler's safety net, with audit events naming the prerequisite.
- Persistence: edges (10) and the blocked state survived restarting PostgreSQL and TaskFlow (`migrations complete: 0 applied`); completing the prerequisite afterwards released the dependent.
- Redis `FLUSHALL` with a blocked job in the graph: after repair the ready queues held exactly the READY jobs, and the blocked job was in the delayed set, not in a ready queue.
- Graceful shutdown: scheduler stopped, exit code 0.

## Performance Benchmarks

Baseline measured once on a shared Codespace VM; full tables, environment and caveats in `docs/benchmarks/dag-phase4.md`.

| Measurement | Result |
|---|---|
| Cycle check, 100,000-node chain / 100,000-edge wide graph / 297,000-edge layered graph | 0.9 ms / 0.4 ms / 2.6 ms |
| Topological sort, same graphs | 3.2 ms / 1.6 ms / 6.1 ms (16–32 ns per edge) |
| `CreateWorkflow`, 1,000 jobs + 1,900 edges in one transaction | 958 ms (1,044 jobs/s, 1,983 edges/s) |
| `AddDependencies` onto a chain growing to 2,000 deep | p50 8.3 ms, p95 136 ms, p99 154 ms |
| `ValidateEdges` finding a cycle through a 2,001-job chain | median 8.0 ms |
| Failure propagation: 1,000 dependents / chain of 500 | 94 ms (10,633 jobs/s) / 675 ms (740 jobs/s) |
| Readiness propagation through the scheduler (10 layers × 200 jobs) | 1,440 jobs/s in this run (1,440–1,902 across three runs) |
| Phase 3 scheduler benchmark re-run with the dependency filter | 8,272–8,750 jobs/s (six runs) vs 9,642 in Phase 3 |

The dependency filter costs about 3–5 µs per candidate row (so the scheduler is ~10–15% slower for the same workload); `AddDependencies` has an unexplained bimodal latency (about 5% of calls take 130–200 ms). The numbers are indicative, not production capacity.

## Known Issues

1. **Phase 4 CI not yet observed** (see Testing Summary).
2. **Dependency submission is serialized** by one advisory lock (deliberate, for cycle safety); its throughput is one transaction at a time. Reads, scheduling and execution are unaffected.
3. **Cycle check cost grows with the stored ancestry** of the new prerequisites (bounded by 500,000 examined edges; larger graphs are rejected, not truncated).
4. **`AddDependencies` latency is bimodal** (about 5% of calls 130–200 ms); cause not investigated.
5. **Failure propagation down a long chain is slower per job than across a fan-out** (level by level: 740 vs 10,633 jobs/s).
6. **`ResolveBlocked` scans the edges table** once per reconcile interval (default 10 ticks); fine to moderate graph sizes, to be indexed/streamed from Phase 9 measurements.
7. **The scheduler's dependency check costs ~3–5 µs per candidate** (above). It is linear in candidates and independent of table size.
8. **Per-job limits:** 1,000 prerequisites per job (checked per request; the limit is not re-counted against edges already stored).
9. **The dependency filter relies on the planner honoring a subquery fence** (`OFFSET 0`); a test asserts the plan shape, so a PostgreSQL planner change would be caught.
10. Carried over: Redis lag after outages, reconciliation holds full sets in memory, `Redis pop is not a claim`, requeued jobs keep their attempt count, audit events depend on the repository (a raw SQL change records none), `/health` latency is unbounded if a hostname stops resolving, `ctest` counts skips as passes (none occurred), Linux only.
11. The Codespace VM needed two kernel settings for the tests (`net.bridge.bridge-nf-call-iptables=0`, `vm.mmap_rnd_bits=28`); both are Codespace quirks, not project requirements.
12. Local Windows Docker was not used for Phase 4 validation, per the Codespaces rule.

## Important Decisions

| Date | Decision |
|------|----------|
| 2026-10-07 | Locked 10-phase roadmap, architecture, tech stack; PostgreSQL authoritative, Redis rebuildable; at-least-once with fencing; single binary with role flags; commits only as `iamankoo`. |
| 2026-10-07 | vcpkg; libpq and hiredis used directly; single `/health`; signals on a `sigwait` thread; integration tests fail rather than skip. |
| 2026-10-07 | Eight-state job model (owner instruction), enforced in C++ and by a database trigger. |
| 2026-10-13 | Phase 3: heap decides promotion order; aging = one level per interval waited (clock-independent key); Redis sorted sets with an atomic Lua dequeue; due detection on PostgreSQL; `SKIP LOCKED` promotion; no Phase 3 migration. See `docs/SCHEDULER.md`. |
| 2026-10-13 | **Dependency readiness is derived from the prerequisites' real states, not a stored `unmet_dependencies` counter.** Rationale: a counter can drift (manual requeue, repair), `COMPLETED` is permanent so the derived answer is monotone, and promotion re-checks under the row lock anyway. `ARCHITECTURE.md` and `Phases.md` updated. |
| 2026-10-13 | **The database enforces readiness**: a trigger refuses any transition to `READY` while a prerequisite is not `COMPLETED`, so neither Redis nor any writer can bypass the DAG. |
| 2026-10-13 | **Cycle safety by a transaction-scoped advisory lock** serializing graph writes across instances (not SERIALIZABLE, not a process mutex); graph writes only, so reads and scheduling are unaffected. Deadlocks/serialization failures are retried (5 attempts, randomized backoff). |
| 2026-10-13 | **Failure policy:** failed/timed-out prerequisite → dependents `FAILED`; cancelled prerequisite → dependents `CANCELLED`; transitive; same transaction as the triggering transition; `RETRY_WAIT` does not propagate; ended dependents are not revived by a requeue. Adds `PENDING→FAILED`. |
| 2026-10-13 | **Prerequisite foreign key is `DEFERRABLE INITIALLY DEFERRED`** (found by a test): a plain `NO ACTION` check can run before the cascade from a dependent removed its edge, making workflow deletion depend on row order. |
| 2026-10-13 | **Dependency filter written as a fenced `NOT EXISTS` with a scalar primary-key lookup** (found by an A/B measurement): the plain-join form could be planned as a whole-table hash join when `job_dependencies` had no statistics; `LIMIT 1` does not work as a fence (PostgreSQL strips it from `EXISTS`). |
| 2026-10-13 | `CreateWorkflow` validates shape and cycles in memory with stand-in ids first, creates jobs in topological order (so sequence numbers follow dependency order), then adds edges under the graph lock, all in one transaction. |
| 2026-10-13 | Docker validation for Phase 4 ran in GitHub Codespaces, not on the local Windows Docker, per the owner's rule. |

## Git Commit History

| Commit | Message | Phase |
|--------|---------|-------|
| `585b4b5` | docs: initialize TaskFlow architecture and development roadmap | Initialization |
| `75e2548` | feat: complete TaskFlow phase 1 foundation | Phase 1 |
| `c523e2e` | feat: complete TaskFlow phase 2 job persistence | Phase 2 |
| `6227a7a` | feat: complete TaskFlow phase 3 scheduler | Phase 3 |
| _next commit on `main`_ | feat: complete TaskFlow phase 4 DAG dependencies | Phase 4 |

The Phase 4 commit hash is reported in the Phase 4 completion report and written into this table by the first commit of Phase 5.

## Next Phase

**Phase 5 — Concurrent Worker Pool** (NOT STARTED; begins only on the owner's instruction).
