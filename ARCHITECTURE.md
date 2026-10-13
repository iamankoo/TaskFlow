# TaskFlow — Architecture (Locked)

Governed by `Phases.md` (roadmap authority). Technology choices are in `TECHSTACK.md`. This document describes the **intended** architecture; `summary.md` records what is actually implemented. Changes require an explicit recorded decision (see `Phases.md`).

## 1. System Architecture

```
Clients
   ↓
REST API
   ↓
TaskFlow Core
   ├── Scheduler
   ├── Priority Queue
   ├── Delayed Jobs
   ├── DAG Dependency Engine
   ├── Retry Manager
   ├── Lease Manager
   └── Worker Manager
          ↓
     Worker Pool
          ↓
PostgreSQL + Redis
```

TaskFlow ships as **one C++20 binary with role flags** (`--role=api|scheduler|worker|all`). `all` runs every role in one process (development); in production each role runs as its own container so roles scale and fail independently. All roles are stateless; durable state lives in PostgreSQL, ephemeral coordination state in Redis.

## 2. Guiding Principles

1. **PostgreSQL is the durable source of truth.** Every job, dependency, attempt, lease, worker record, and audit event is persisted there. If PostgreSQL says a job is in state X, it is in state X.
2. **Redis is an accelerator, not a database.** Redis holds queues, delayed-job index, heartbeat TTL keys, caches, and short-lived locks. Redis can be flushed at any time with **zero job loss**: queues are rebuilt from PostgreSQL by reconciliation.
3. **Write PostgreSQL first, then Redis.** A job is never visible in Redis unless it is already committed in PostgreSQL.
4. **At-least-once execution**, with idempotency controls and fencing tokens that make duplicates safe and stale writers harmless. Exactly-once is not claimed.
5. **Every state change goes through the state machine** and is recorded in an append-only event log in the same transaction.

## 3. Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **REST API** (Drogon) | Authenticate, validate, translate HTTP to core operations. Contains no scheduling or execution logic. |
| **Scheduler** | Periodic loop: promote due delayed jobs, release dependency-ready jobs, keep Redis queues consistent with PostgreSQL, run reconciliation. |
| **Priority Queue** | Redis-backed ready queues ordered by priority, then `run_at`, then submission sequence. |
| **Delayed Jobs** | Redis sorted set scored by `run_at`; due entries are atomically moved to the ready queue. Also used for retry backoff. |
| **DAG Dependency Engine** | Validates graphs (cycle detection), tracks unmet-dependency counts, releases children when all parents succeed, propagates failure. |
| **Retry Manager** | Decides retry vs. dead-letter from the job's policy; computes exponential backoff with jitter; re-schedules as a delayed job. |
| **Lease Manager** | Grants, renews, and expires leases; issues fencing tokens; sweeps expired leases and reclaims orphaned jobs. |
| **Worker Manager** | Worker registration/deregistration, capacity tracking, liveness state (`ACTIVE`, `DRAINING`, `SUSPECT`, `DEAD`). |
| **Worker Pool** | Bounded thread pool that executes handlers, sends heartbeats, reports results. |
| **PostgreSQL** | Durable state, transactions, constraints, audit trail. |
| **Redis** | Fast coordination (see §14). |

## 4. Job Lifecycle

1. **Submit.** API validates the request, applies the idempotency key, and inserts the job (and, from Phase 4, its dependency edges) in one PostgreSQL transaction together with its creation event.
2. **Initial state** is chosen: `PENDING` (`scheduled_at` in the future, or unmet dependencies) or `READY` (runnable now).
3. **Release.** When `scheduled_at` arrives or dependencies complete, `PENDING`→`READY`, and the job is published to the Redis ready queue.
4. **Claim.** A worker pulls a job. In one PostgreSQL transaction the job moves `READY`→`RUNNING`, `attempt_count` is incremented, and a `job_attempts` row is written (`JobRepository::CreateAttempt`). From Phase 6 the same transaction also creates the lease with its fencing token.
5. **Run.** The worker executes the handler, heartbeating to keep the lease alive.
6. **Complete.** The worker reports the outcome (with its fencing token from Phase 6). In **one transaction** the result is stored, the attempt is closed and the job moves on (`JobRepository::StoreResult`): success → `COMPLETED`; failure or timeout with attempts remaining → `RETRY_WAIT` (`scheduled_at` = now + backoff); failure that is final → `FAILED`; final attempt timed out → `TIMEOUT`.
7. **Retry.** When `scheduled_at` arrives, `RETRY_WAIT`→`READY` and the cycle repeats from step 4.
8. **Propagate.** A job with prerequisites stays `PENDING` until every prerequisite is `COMPLETED`; the scheduler then promotes it like any other due job. When a prerequisite ends `FAILED` or `TIMEOUT`, its `PENDING` dependents end `FAILED`; when it ends `CANCELLED`, they end `CANCELLED`, transitively, in the same transaction (section 9).

## 5. Job State Machine

States: `PENDING`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `RETRY_WAIT`, `TIMEOUT`, `CANCELLED`.
Final states (no automatic transition leaves them): `COMPLETED`, `FAILED`, `TIMEOUT`, `CANCELLED`. `FAILED` and `TIMEOUT` can be requeued manually; `COMPLETED` and `CANCELLED` are permanent.

| From | To | Trigger |
|------|----|---------|
| (new) | `PENDING` | Submitted with a future `scheduled_at` (or, from Phase 4, unmet dependencies) |
| (new) | `READY` | Submitted and runnable now |
| `PENDING` | `READY` | `scheduled_at` reached **and** every prerequisite `COMPLETED` |
| `PENDING` | `CANCELLED` | Cancel request, or a prerequisite was cancelled |
| `PENDING` | `FAILED` | A prerequisite failed or timed out, so the job can never run |
| `READY` | `RUNNING` | Worker claims the job (creates an attempt) |
| `READY` | `CANCELLED` | Cancel request |
| `RUNNING` | `COMPLETED` | Attempt succeeded |
| `RUNNING` | `RETRY_WAIT` | Attempt failed or timed out and attempts remain |
| `RUNNING` | `FAILED` | Attempt failed and the failure is final (non-retryable or attempts exhausted) |
| `RUNNING` | `TIMEOUT` | Final attempt exceeded its timeout |
| `RUNNING` | `CANCELLED` | Cancel request (the active attempt is closed as cancelled) |
| `RETRY_WAIT` | `READY` | Backoff elapsed (`scheduled_at` reached) |
| `RETRY_WAIT` | `CANCELLED` | Cancel request |
| `FAILED` | `READY` | Manual requeue |
| `TIMEOUT` | `READY` | Manual requeue |

These 14 transitions are the only legal ones; everything else (for example `COMPLETED`→`RUNNING`, `CANCELLED`→`RUNNING`, `PENDING`→`RUNNING`, `RETRY_WAIT`→`RUNNING`) is rejected. The table is enforced twice: by the C++ transition function (`src/jobs/job_state.cpp`) and by a PostgreSQL trigger on `jobs` (`migrations/0001_create_jobs.sql`); an integration test compares both for all 56 ordered pairs of distinct states. Each transition also writes a `job_events` row in the same transaction. No job can become `READY` while a prerequisite is not `COMPLETED`: the same trigger enforces that for every writer (scheduler, operator, stale Redis entry). A requeued job keeps its `attempt_count`; granting more attempts is an explicit edit of its retry policy.

## 6. Scheduler Architecture

The Scheduler (`docs/SCHEDULER.md`) runs a periodic tick (configurable) that is safe to run on multiple instances: promotion is guarded by PostgreSQL row locks (`FOR UPDATE SKIP LOCKED`), Redis writes are idempotent, and only reconciliation takes a short-lived, TTL-bounded Redis lock. Multiple schedulers cooperate rather than conflict. The scheduler changes job state only by `PENDING`/`RETRY_WAIT` → `READY`.

Each tick:
1. **Find due jobs** in PostgreSQL (`PENDING`/`RETRY_WAIT` with `scheduled_at <= now` and no unmet prerequisite, served by `idx_jobs_due`).
2. **Decide**: order the candidates with the in-memory priority heap (priority, FIFO, aging) and take the best N.
3. **Promote** those N to `READY` in one PostgreSQL transaction, with one `job_events` row each.
4. **Publish** the promoted jobs to the Redis ready queues (one Lua script also removes them from the delayed set).
5. **Reconcile** periodically, immediately if Redis looks flushed (missing sentinel), or after a Redis error: repair Redis from PostgreSQL (re-enqueue missing, drop stale).
6. Periodically end `PENDING` jobs stranded behind a failed prerequisite (a safety net; the normal path already did it). *(Phase 6)* sweep expired leases.

PostgreSQL commits before Redis is touched; a Redis failure never blocks or undoes a promotion.

## 7. Priority Scheduling

- Priority levels (highest first): `CRITICAL`, `HIGH`, `NORMAL`, `LOW`; ordering is defined once, in `PriorityPolicy`.
- Order: priority, then earliest `scheduled_at`, then submission `sequence` (FIFO).
- The scheduling decision uses an indexed binary heap (`JobPriorityQueue`: push/pop/remove O(log n), duplicate-protected, supports invalidating stale entries). PostgreSQL identifies *eligible* jobs; it does not decide the order.
- Redis keeps one sorted set per priority level (`ready:CRITICAL` … `ready:LOW`, score `scheduled_at`, member `<sequence>:<id>`). A single Lua script dequeues the best head across the four sets using exactly the same rule, including aging; a test checks it against the C++ heap.
- **Aging:** every `aging_interval` a job has waited counts as one extra priority level (measured from `scheduled_at`), so a `LOW` job overtakes a fresh `CRITICAL` one after three intervals. This is the continuous form of "promoted one level after a threshold", and it has a time-independent sort key (`scheduled_at - level × interval`), which keeps the heap valid as time passes. Aging influences ordering only; the persisted priority is never changed. Interval 0 disables it.

## 8. Delayed Jobs

Delayed jobs (`scheduled_at` in the future, and retry backoffs) live in the PostgreSQL row (`state = PENDING`, or `RETRY_WAIT` for backoff, with `scheduled_at`) as truth, and in a Redis sorted set (`delayed`) scored by `scheduled_at` as a fast index. Due detection uses PostgreSQL's `idx_jobs_due`, so losing Redis never delays a promotion. Promotion moves the job out of the delayed set into its ready queue atomically (Lua). If Redis is lost, reconciliation rebuilds the sorted sets from PostgreSQL.

Redis holds only identifiers and sort keys; PostgreSQL wins every disagreement.

## 9. DAG / Dependency Execution

Detailed design: `docs/DATABASE.md` ("Dependencies").

- **Model.** Dependencies are edges `job_dependencies (job_id, depends_on_id)`: *job waits for depends_on*. The graph must be a DAG. Fan-out, fan-in, diamonds and independent components are all just edge sets. Limits: 1,000 prerequisites per job, 100,000 edges per request, 10,000 jobs per workflow, 500,000 stored edges examined per cycle check.
- **Submission and cycle safety.** All edges of a request are validated and stored in **one transaction** or not at all. A transaction-scoped PostgreSQL advisory lock serializes every graph-changing transaction across all processes, so two requests that each add half of a cycle cannot both pass the check. The check loads the stored ancestors of the new prerequisites, adds the new edges, and runs an iterative depth-first cycle search (`DependencyGraph`, O(V + E), no recursion); a cycle is rejected with the cycle path. Topological sorting uses Kahn's algorithm, O(V + E).
- **Readiness is derived, not counted.** A job is releasable when every prerequisite is `COMPLETED`. This is computed from the edges and the prerequisites' actual states, never from a stored counter, so it cannot drift (for example after a manual requeue). Because `COMPLETED` is permanent, the answer only moves from "no" to "yes". A job with prerequisites stays `PENDING`; the scheduler promotes it (when its own `scheduled_at` has also passed) with the normal `FOR UPDATE SKIP LOCKED` transaction, which re-checks the prerequisites under the row lock. That makes the release **exactly once**, however many parents finish simultaneously and however many schedulers run. The database trigger additionally refuses any other route to `READY`.
- **Failure propagation (policy).** When a job ends `FAILED`, `TIMEOUT` or `CANCELLED`, the transaction that makes the transition also ends every `PENDING` job that transitively waits for it: prerequisite `FAILED`/`TIMEOUT` → dependent `FAILED` (`PENDING`→`FAILED`); prerequisite `CANCELLED` → dependent `CANCELLED`. One `job_events` row per job records the reason ("prerequisite <id> failed"). A retryable failure (`RETRY_WAIT`) does **not** propagate: the prerequisite may still succeed. Dependents that are no longer `PENDING` are left alone. Adding an edge to an already failed prerequisite ends the new dependent immediately.
- **Manual requeue.** Requeuing a failed prerequisite never resurrects its ended dependents, and they cannot be requeued until every prerequisite has actually completed (the trigger refuses `READY` otherwise).
- **Safety net.** `DependencyService::ResolveBlocked` finds `PENDING` jobs behind an already ended prerequisite (for example after a manual database edit) and ends them; the scheduler runs it with its periodic pass.
- Supported shapes: chains, fan-out, fan-in, diamonds, layered graphs.

## 10. Worker Pool and Registration

- On start, a worker **registers** (id, hostname, capacity, supported job types, version) in PostgreSQL and announces liveness in Redis.
- The worker owns a bounded executor (thread pool + bounded queue) providing backpressure; it **pulls** work only when it has free capacity.
- A handler registry maps job `type` to handler. Handlers receive `job_id`, `attempt`, payload, and a cancellation token.
- Workers deregister on clean shutdown; otherwise the failure detector marks them dead.

## 11. Leasing, Heartbeats, and Failure Detection

- **Lease:** claiming a job creates a lease (`lease_owner`, `lease_expires_at`, `lease_token`) in PostgreSQL. The lease token is a monotonically increasing **fencing token**.
- **Heartbeat:** each worker periodically heartbeats, renewing the leases of its running jobs and refreshing a Redis TTL key for fast liveness detection. PostgreSQL records `last_heartbeat_at` durably.
- **Fencing:** every state write by a worker (start, complete, fail, renew) must present its token; the write is rejected if the token is not current. A zombie worker therefore cannot overwrite a newer attempt.
- **Failure detection:** the lease sweeper finds jobs whose lease expired, and workers with no heartbeat (`ACTIVE`→`SUSPECT`→`DEAD`). Detection bound ≈ lease TTL + sweep interval.
- **Reclaim:** expired jobs are returned via the Retry Manager (`RETRY_WAIT`) or finished as `FAILED`/`TIMEOUT`, counting as a failed attempt.

## 12. Retries, Timeouts, and Idempotency

- **Retry:** per-job `max_attempts`, exponential backoff with jitter, retryable/non-retryable classification. Retries are delayed jobs. Every attempt is stored in `job_attempts`.
- **Dead-letter:** exhausted jobs end `FAILED` (or `TIMEOUT`), retained for inspection and manual requeue.
- **Timeouts:** each job has an execution timeout. The worker enforces it via cooperative cancellation; the lease sweeper enforces it as a backstop for hung or unreachable workers.
- **Idempotency (submission):** a unique `idempotency_key` guarantees duplicate submissions return the original job.
- **Idempotency (execution):** because delivery is at-least-once, handlers receive a stable `job_id` and `attempt` to dedupe external side effects. This contract is documented for handler authors.

## 13. PostgreSQL Responsibilities

Durable source of truth for: `jobs`, `job_dependencies`, `job_attempts`, `workers`, `job_events` (audit), leases and fencing tokens, idempotency keys, retry state. Provides transactions, unique/foreign-key/check constraints, optimistic concurrency (`version`), and `FOR UPDATE SKIP LOCKED` claiming. Schema changes ship as versioned migrations.

## 14. Redis Responsibilities

Redis is used for: ready queues per priority, the delayed-job sorted set, worker heartbeat TTL keys, short-lived distributed locks (e.g., sweeper/reconciler leader election), and small caches (e.g., queue stats). Redis never holds the only copy of any job. Loss or flush of Redis degrades latency until reconciliation completes, but never loses or duplicates committed work.

## 15. REST API Boundary

The API (`/api/v1`) is the only external interface. It performs authentication (API key), validation, and translation to core services; it never touches Redis queues or executes jobs directly. Operations: submit job, submit DAG, get/list/filter jobs, cancel, requeue dead, list workers, stats. Operational endpoints: `/health` (process liveness, PostgreSQL/Redis connectivity and shutdown state; `200` when healthy, `503` when a dependency is down or the process is shutting down) and `/metrics` (Prometheus format). Workers and scheduler interact with core services directly, not through the REST API.

## 16. Concurrency Model

- Multiple API, scheduler, and worker instances run concurrently.
- Cross-process coordination uses PostgreSQL row locks, conditional updates (`WHERE state = ... AND version = ...`), and atomic Redis Lua scripts. No in-process lock is relied on for cross-process correctness.
- In-process: Drogon event loops for I/O; a bounded thread pool for job execution; a connection pool for PostgreSQL; shared state guarded by well-scoped mutexes/atomics; verified under ThreadSanitizer.

## 17. Failure-Recovery Flow

| Failure | Recovery |
|---------|----------|
| Worker crashes mid-job | Heartbeats stop → lease expires → sweeper reclaims job → `RETRY_WAIT` or `FAILED`. Fencing blocks any late write from the old worker. |
| Worker hangs | Execution timeout (cooperative) then lease expiry backstop. |
| Scheduler crashes | Stateless; another instance (or restart) resumes. Row locks/conditional updates prevent double-processing. |
| Redis lost/flushed | Reconciliation rebuilds queues and delayed set from PostgreSQL. |
| PostgreSQL unavailable | Writes fail fast, `/health` returns `503` (`degraded`), workers stop claiming; in-flight jobs finish locally and report on reconnect (fencing validates). |
| API instance crashes | Clients retry using idempotency keys. |

## 18. Data Consistency Model

- Strong consistency for job state: all transitions are single PostgreSQL transactions.
- Redis is eventually consistent with PostgreSQL; divergence is bounded by the reconciliation interval and is always resolved in favor of PostgreSQL.
- Delivery guarantee: **at-least-once**. Duplicate execution is possible only after failure/timeout; duplicate **completion** is prevented by fencing and the state machine.
- Publish ordering: PostgreSQL commit before Redis publish. A crash between them is repaired by reconciliation.

## 19. Graceful Shutdown

On SIGTERM/SIGINT:
1. `/health` starts returning `503` with status `shutting_down` (kept for a configurable grace period so load balancers drain); API stops accepting new requests, completes in-flight ones.
2. Scheduler finishes the current tick and stops.
3. Worker enters `DRAINING`: stops claiming, finishes in-flight jobs up to a deadline, reports results, releases leases of unstarted jobs, deregisters.
4. Jobs still running at the deadline are left to lease expiry (reclaimed by recovery).
5. Close pools and flush logs.

## 20. Observability

- **Logging:** spdlog structured logs with job, worker, and request correlation ids.
- **Metrics (Prometheus-compatible):** queue depth per priority, jobs by state, job latency and queue-wait histograms, worker counts by state, lease expirations, retries, dead-letter count, reconciliation repairs, API request rate/latency/errors.
- **Health:** `/health`.
- **Audit:** `job_events` provides a complete per-job history.

## 21. Docker Deployment Architecture

```
docker compose
 ├── taskflow-api        (role=api,       port 8080)
 ├── taskflow-scheduler  (role=scheduler)
 ├── taskflow-worker xN  (role=worker)
 ├── postgres            (volume: pgdata)
 └── redis               (persistence optional; not authoritative)
```

- Multi-stage Dockerfile: build stage compiles with CMake; runtime stage is minimal and runs as non-root.
- Development profile runs `role=all` as a single `taskflow` service.
- Configuration via environment variables; secrets are never baked into images; `.env.example` documents all variables.
- Health checks on every service; workers scale with `--scale taskflow-worker=N`.
