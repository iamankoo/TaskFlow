# TaskFlow — Living Project Summary

> Updated after **every** completed phase. Only implemented **and validated** work is recorded as done. Authority order: `Phases.md` → `ARCHITECTURE.md` → `TECHSTACK.md` → `summary.md`.

## Project Overview

TaskFlow is a distributed job execution platform in C++20: REST API, priority scheduler, delayed jobs, DAG dependencies, concurrent worker pool, leasing/heartbeat-based failure recovery, retries, timeouts, and idempotency. PostgreSQL is the durable source of truth; Redis provides fast coordination. Repository: https://github.com/iamankoo/TaskFlow

## Current Status

**Phases 1, 2 and 3 complete.** TaskFlow persists jobs durably (Phase 2) and now has a scheduler that decides which persisted job becomes runnable next: delayed jobs are held until their time, due jobs are promoted `PENDING`/`RETRY_WAIT` → `READY` in priority order (FIFO within a priority, with starvation-preventing aging) and queued in Redis, and Redis is rebuilt from PostgreSQL after any loss. 259 tests pass (also under ASan + UBSan, and the scheduler tests under ThreadSanitizer) against real PostgreSQL and Redis; the Compose stack was built and validated live in GitHub Codespaces.

Nothing executes jobs yet and there is no job API: the running server exposes only `GET /health`.

## Current Phase

None in progress. Phase 4 starts only on the owner's instruction.

## Completed Phases

- **Phase 1 — Foundation & Project Infrastructure.** Commit `75e2548`.
- **Phase 2 — Job Model & Persistent State.** Commit `c523e2e`.
- **Phase 3 — Scheduler & Priority Queue.** Commit hash: reported in the Phase 3 completion report and recorded here by the first commit of Phase 4 (a commit cannot contain its own hash).

## Phase-by-Phase Summary

| # | Phase | Status | Commit |
|---|-------|--------|--------|
| 1 | Foundation & Project Infrastructure | **COMPLETE** | `75e2548` |
| 2 | Job Model & Persistent State | **COMPLETE** | `c523e2e` |
| 3 | Scheduler & Priority Queue | **COMPLETE** | see Git Commit History |
| 4 | DAG Dependency Engine | NOT STARTED | — |
| 5 | Concurrent Worker Pool | NOT STARTED | — |
| 6 | Job Leasing, Heartbeats & Failure Recovery | NOT STARTED | — |
| 7 | Retries, Timeouts & Idempotency | NOT STARTED | — |
| 8 | REST API & Production Operations | NOT STARTED | — |
| 9 | Testing, Load Testing & Performance Engineering | NOT STARTED | — |
| 10 | Final Hardening, Benchmark & Release (FINAL) | NOT STARTED | — |

### Phase 1 (summary)

Build system (CMake presets, vcpkg manifest), configuration, logging, error types, PostgreSQL and Redis connection pools, migration runner, Drogon HTTP server with `GET /health`, graceful lifecycle, Docker/Compose, GitHub Actions CI. CI run on GitHub: green on all three jobs.

### Phase 2 (summary)

Job domain model (`Job`, `NewJob`, `JobId`, `RetryPolicy`, attempts, results, events), the eight-state machine (`PENDING`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `RETRY_WAIT`, `TIMEOUT`, `CANCELLED`; 13 legal transitions) enforced both in C++ and by a PostgreSQL trigger with a parity test, schema (`jobs`, `job_attempts`, `job_results`, `job_events`) with CHECK/UNIQUE/foreign-key constraints and scheduler-ready indexes, transactional `JobRepository`, idempotency-key storage. Details: `docs/DATABASE.md`. CI run on GitHub: green on all three jobs.

### Phase 3 detail

**Objective.** Determine which persisted job should become executable next, by priority and scheduling time: scheduler, priority queue, delayed jobs, ready-job promotion, aging, Redis scheduling structures and PostgreSQL↔Redis reconciliation. Design and rationale: `docs/SCHEDULER.md`.

**Scheduler** (`include/taskflow/scheduler/`, `src/scheduler/`). One `Tick()`: (1) read due `PENDING`/`RETRY_WAIT` jobs from PostgreSQL via `idx_jobs_due`, (2) order them with the in-memory priority heap and take the best `max_promotions_per_tick`, (3) promote them to `READY` in one transaction with `FOR UPDATE SKIP LOCKED` and one `job_events` row each, (4) publish them to the Redis ready queues, (5) reconcile Redis if due. `Start()`/`Stop()` run the loop on a thread that waits on a condition variable (no busy loop) and stops promptly. The scheduler is independent of HTTP and the repository (which only retrieves and persists), takes an injectable clock for deterministic tests, and is started by `Application` when `scheduler.enabled` (default true). A Redis failure never blocks or undoes a promotion; it is reported in the tick result and repaired by the next reconciliation.

**Priority queue.** `JobPriorityQueue`: indexed binary min-heap, one entry per job (duplicate pushes replace and re-sort), `Push`/`Pop`/`Remove` O(log n), `Top`/`Contains` O(1), `RemoveIf` O(n) for stale entries. Ordering is defined once in `PriorityPolicy`.

**Priority levels and FIFO.** `CRITICAL` > `HIGH` > `NORMAL` > `LOW`; the numeric mapping exists in one function and everything else iterates `kPrioritiesHighestFirst`. Equal priority: earlier `scheduled_at`, then lower `sequence` (Phase 2 submission order), so `#10 → #11 → #12`.

**Delayed jobs.** A job with `scheduled_at > now` is never promoted; due means `scheduled_at <= now` (exact to the microsecond; tested one microsecond before and at the boundary). Due jobs are found with the Phase 2 `idx_jobs_due` index (a test asserts the planner uses it); there is no polling of every row. Retry-backoff jobs (`RETRY_WAIT`) are promoted the same way.

**Aging.** Every `aging_interval_ms` (default 60 s; 0 disables) a job has waited counts as one extra priority level, measured from `scheduled_at`. This has a clock-independent sort key (`scheduled_at − level × interval`), so a plain heap stays valid as time passes. A `LOW` job overtakes a fresh `CRITICAL` one after three intervals. The persisted priority is never modified (tested).

**Redis structures.** Identifiers and sort keys only, never job data: `{prefix}:ready:CRITICAL|HIGH|NORMAL|LOW` and `{prefix}:delayed` are sorted sets (member `<20-digit sequence>:<uuid>`, score `scheduled_at` in µs), `{prefix}:lock:reconcile` is a short-lived TTL lock, `{prefix}:sentinel` marks a reconciled Redis. A Lua script (`PopNext`) atomically dequeues the best head across the four queues with the same rule (including aging) as the C++ policy; a test compares both on random data. Why sorted sets, why padded members, why one set per priority: `docs/SCHEDULER.md`.

**Reconciliation (PostgreSQL wins).** Pages through READY and PENDING/RETRY_WAIT jobs (keyset on `(scheduled_at, sequence)`) and through Redis, then adds what is missing or wrongly scored and removes what is stale, re-checking each removal against PostgreSQL first. Handled and tested: READY missing from Redis; entries for COMPLETED/CANCELLED/unknown jobs; wrong priority queue; READY job in the delayed set; wrong score; malformed members; total loss (`FLUSHDB`, container restart). Redis loss is noticed within one tick (missing sentinel). It never modifies job state.

**Multi-scheduler safety.** Promotion is guarded by PostgreSQL (`SKIP LOCKED` + status predicate re-checked under the lock): a job is promoted by exactly one scheduler, with no global lock. Redis writes are idempotent. Only reconciliation takes a Redis lock: short-lived (`lock_ttl_ms`), owner-token checked on release, expires by itself if its holder crashes. Test: 5 schedulers × the same 100 jobs → 100 promotions, 100 `READY` events (one per job), every row at version 2, no other states, Redis converged to exactly 100 unique entries.

**Database changes.** None. The Phase 2 schema and indexes were sufficient; new repository queries only (`FindDueJobs`, `PromoteJobsToReady`, `FindReadyJobs`, `FindWaitingJobs`, `GetSchedulingEntries`; none read payloads). No `job_dependencies`, `workers` or lease tables.

**Not built (later phases):** DAG dependencies (4), workers and job execution (5), leases/heartbeats (6), retries/timeouts/idempotency logic (7), any job API (8).

## Implemented Features

- Everything from Phases 1–2.
- Scheduler tick and background loop, priority heap, priority/FIFO/aging policy, delayed-job promotion, Redis ready/delayed queues with atomic Lua dequeue, reconciliation and Redis-loss recovery, multi-scheduler safe promotion, scheduler configuration (`scheduler.*` / `TASKFLOW_SCHEDULER_*`), baseline benchmark tool.

## Architecture Summary

As in `ARCHITECTURE.md` (sections 4–8 describe the lifecycle, state machine and scheduler as implemented). Implemented: infrastructure (Phase 1), persistent job layer (Phase 2), scheduler and queues (Phase 3). Not yet: DAG engine, retry manager, lease manager, worker manager, worker pool.

## Technology Stack

Locked (see `TECHSTACK.md`): C++20, CMake, Drogon, PostgreSQL, Redis, Docker, Docker Compose, GoogleTest, spdlog, JSON, Prometheus-compatible metrics, GitHub Actions. Package manager: vcpkg (manifest mode, baseline `4b308f5`). No new dependencies in Phase 3. Redis Lua scripting (built into Redis) is used for atomic dequeue/move.

## Database Summary

Unchanged from Phase 2: `schema_migrations`, `jobs`, `job_attempts`, `job_results`, `job_events` (4 migrations). Phase 3 query/index usage is documented in `docs/DATABASE.md` ("Scheduler queries"). `job_dependencies` arrives in Phase 4 and `workers` in Phase 5.

## API Summary

| Endpoint | Behavior |
|---|---|
| `GET /health` | Unchanged: `200` with dependency status, or `503` `degraded`/`shutting_down`. |

No job endpoints yet (Phase 8). The scheduler is configured and observed through configuration, logs and the database/Redis.

## Testing Summary

**259 tests, all passing**, against real PostgreSQL 16 and Redis 7 (no mocks of either):

| Suite | Total | New in Phase 3 | Phase 3 coverage |
|---|---|---|---|
| `taskflow_unit_tests` | 99 | 26 | heap: priority order, FIFO, empty/duplicate/stale entries, removal, 50,000-entry sorted-order check, randomized comparison with a reference model (aging on and off); policy: aging overtake, short waits keep priority, clock independence, no starvation under continuous CRITICAL traffic, priority never rewritten; Redis member encoding; scheduler config |
| `taskflow_integration_tests` | 152 | 55 | repository scheduling queries (due boundary to the µs, per-priority limits, promotion with audit events, keyset paging, index use by the planner, 8 concurrent promoters); Redis queues (order, FIFO ties, aging, Lua pop vs C++ heap on random data, atomic move, chunking, locks incl. expiry and owner check, flush); scheduler (due vs delayed, boundary, time advancing, priority and FIFO order across ticks, retry-wait promotion, untouched states, aging and persisted priority, Redis flush recovery, stale/misplaced/wrong-score repair, paging, lock contention and expiry, Redis-down tolerance, 5 concurrent schedulers × 100 jobs, loop start/stop/prompt shutdown/tick timing) |
| `taskflow_http_tests` | 8 | 1 | the real application runs the scheduler thread, which promotes a due job and queues it in Redis; shutdown stops the scheduler |

Results (all in GitHub Codespaces):

- Release build with `-Werror` (`ci` preset): **259/259 pass**, 0 skipped.
- ASan + UBSan build: **259/259 pass**, 0 skipped, no sanitizer reports.
- ThreadSanitizer, scheduler/queue/pool tests (84 tests): **all pass**, no data-race reports. The remaining suites were not run under TSan.
- CI on GitHub for Phases 1 and 2: green on all three jobs each. **The Phase 3 CI run had not finished when this was written.**

**Docker/Codespaces validation (live stack, Codespace `standardLinux32gb`: 4 vCPU, 16 GB, Docker 29.8, Compose 5.5), scheduler tick 200 ms, 2 promotions per tick:**

- PostgreSQL, Redis and TaskFlow built and reached healthy; log shows `migrations complete: 4 applied` and the scheduler started; `/health` `200`.
- Inserted 2 delayed (+60 s) and 8 due jobs directly into PostgreSQL: the delayed jobs stayed `PENDING`; all 8 due jobs became `READY`.
- Promotion order by `job_events` id was priorities `3 3 2 2 1 1 0 0` (CRITICAL, CRITICAL, HIGH, HIGH, NORMAL, NORMAL, LOW, LOW); 8 READY events for 8 distinct jobs.
- Redis held 2 jobs in each ready queue and 2 in the delayed set. After `FLUSHALL` the scheduler restored all of it from PostgreSQL within seconds (log: `reconciled Redis (+8 ready … +2 delayed)`); the sum of job versions was identical before and after (18 = 18). After restarting the Redis container (no persistence) the same counts reappeared.
- Setting the delayed jobs' `scheduled_at` to the past promoted them (all 10 jobs READY, delayed set empty); persisted priorities were untouched.
- `docker compose stop taskflow`: logs `scheduler … stopped` then `shutdown complete`, exit code 0.

## Performance Benchmarks

First baseline, measured once (details, environment and caveats in `docs/benchmarks/scheduler-phase3.md`; Phase 9 does the formal benchmark). 5,000 jobs, one scheduler, PostgreSQL and Redis in containers on the same 4-vCPU VM:

| Measurement | p50 | p95 | p99 |
|---|---|---|---|
| Heap push, 100k jobs | 0.28 µs | 0.75 µs | 2.56 µs |
| Heap pop (highest priority), 100k jobs | 1.48 µs | 2.18 µs | 2.54 µs |
| Redis queue insert (one job) | 57.0 µs | 77.9 µs | 100.7 µs |
| Redis queue extract (Lua `PopNext`) | 89.5 µs | 115.7 µs | 136.8 µs |
| Redis queue insert, batch of 500 | 0.80 ms | 0.96 ms | 1.24 ms |
| Scheduler tick (500 promotions + publish) | 46.0 ms | 101.7 ms | 101.7 ms |
| PostgreSQL promotion, batch of 500 | 36.6 ms | 39.2 ms | 39.2 ms |
| PostgreSQL promotion, per job | 73.2 µs | 78.4 µs | 78.4 µs |

**Scheduling throughput: 9,642 jobs/sec** (5,000 promoted and published in 0.519 s). **Reconciliation after `FLUSHDB`: 10,000 ready entries rebuilt in 41.9 ms.** The tick and promotion rows have only 10 samples each, so their percentiles are indicative.

## Known Issues

1. **Phase 3 CI not yet observed** (see Testing Summary); Phases 1–2 runs were green.
2. **Reconciliation is not streaming.** It holds the expected and actual sets in memory; fine for hundreds of thousands of jobs, to be revisited with Phase 9 measurements.
3. **Candidate window.** Each tick reads at most `candidates_per_priority` due jobs per priority (oldest first); the heap order is exact within that window, and a larger backlog drains over several ticks.
4. **Redis pop is not a claim.** `PopNext` removes an entry from Redis but the job stays `READY` in PostgreSQL until Phase 5's worker wins `CreateAttempt`; reconciliation re-adds an unclaimed job. Duplicate or stale queue entries are therefore harmless but can cause a wasted claim attempt.
5. **Briefly stale Redis.** Between a PostgreSQL commit and the Redis publish (or after a Redis outage) Redis can lag by up to one reconcile interval (`reconcile_interval_ticks × tick_interval_ms`, default 10 s), and a transient extra entry is removed by the next reconciliation.
6. **Due detection uses PostgreSQL every tick** (by design, so a flushed Redis never delays promotion); the Redis delayed set is an index for consumers and inspection, not the due-detection path.
7. **Requeued jobs keep their attempt count** (Phase 2; decision deferred to Phase 7). **The transition table will gain edges** (Phase 4 `PENDING→FAILED`; possibly a Phase 6 reclaim edge), each requiring the C++ table, the trigger migration and the parity test to change together.
8. **Audit events depend on the repository**; a manual `psql` state change records none.
9. `/health` latency is unbounded when a dependency hostname stops resolving (planned for Phase 8). `ctest` counts a skipped test as a pass (no test skipped in any final run).
10. Linux only. The Codespace VM needed two kernel settings for the tests (`net.bridge.bridge-nf-call-iptables=0` for container networking, `vm.mmap_rnd_bits=28` for TSan); both are Codespace environment quirks, not project requirements (CI already applies the second).
11. Local Windows Docker was not used for Phase 3 validation, per the Codespaces rule.

## Important Decisions

| Date | Decision |
|------|----------|
| 2026-10-07 | Locked 10-phase roadmap, architecture, and tech stack. |
| 2026-10-07 | PostgreSQL = durable source of truth; Redis = rebuildable coordination layer. |
| 2026-10-07 | Single binary with role flags; roles arrive with their phases. |
| 2026-10-07 | At-least-once delivery with fencing tokens; exactly-once is not claimed. |
| 2026-10-07 | All commits/pushes use `iamankoo <aniketraj00384@gmail.com>` only; no AI attribution. |
| 2026-10-07 | vcpkg (manifest mode, pinned baseline); libpq and hiredis used directly; single `/health`; signals on a `sigwait` thread; integration tests fail rather than skip. |
| 2026-10-07 | Eight-state job model (owner instruction), enforced in C++ and by a database trigger; `job_dependencies` → Phase 4, `workers` → Phase 5. |
| 2026-10-13 | **Scheduling decision lives in an in-memory heap**, not in a database `ORDER BY`: PostgreSQL only finds eligible jobs; `PriorityPolicy` + `JobPriorityQueue` choose which are promoted. |
| 2026-10-13 | **Aging is "one priority level per interval waited"**, the continuous form of the architecture's "one level after a threshold". It yields a clock-independent sort key (`scheduled_at − level × interval`), so a heap and a Redis script can evaluate it exactly. `ARCHITECTURE.md` section 7 updated. |
| 2026-10-13 | **Redis ready queues are sorted sets** (one per priority, score `scheduled_at` µs, member `<padded sequence>:<uuid>`), not lists, so delayed/retried jobs come out in `scheduled_at` order and ties are FIFO; dequeue is one Lua script reproducing `PriorityPolicy`. Redis stores identifiers only. |
| 2026-10-13 | **Due detection stays on PostgreSQL** (`idx_jobs_due`) every tick so Redis loss can never delay promotion; the Redis delayed set is a rebuildable index. |
| 2026-10-13 | **Promotion = one `SKIP LOCKED` transaction**; no scheduler leader and no global lock. Only reconciliation takes a short, TTL-bounded, owner-checked Redis lock. |
| 2026-10-13 | **No schema migration in Phase 3**: Phase 2 indexes already serve every scheduler query (verified with `EXPLAIN` tests). |
| 2026-10-13 | The scheduler runs inside the application process when `scheduler.enabled` (default true); it is started before the HTTP loop and stopped after it. A separate scheduler role arrives with the Phase 8 role flags. |
| 2026-10-13 | Docker validation for Phase 3 ran in GitHub Codespaces, not on the local Windows Docker, per the owner's rule. |

## Git Commit History

| Commit | Message | Phase |
|--------|---------|-------|
| `585b4b5` | docs: initialize TaskFlow architecture and development roadmap | Initialization |
| `75e2548` | feat: complete TaskFlow phase 1 foundation | Phase 1 |
| `c523e2e` | feat: complete TaskFlow phase 2 job persistence | Phase 2 |
| _next commit on `main`_ | feat: complete TaskFlow phase 3 scheduler | Phase 3 |

The Phase 3 commit hash is reported in the Phase 3 completion report and written into this table by the first commit of Phase 4.

## Next Phase

**Phase 4 — DAG Dependency Engine** (NOT STARTED; begins only on the owner's instruction).
