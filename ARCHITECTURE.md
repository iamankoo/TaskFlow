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

1. **Submit.** API validates the request, applies the idempotency key, and inserts the job (and dependency edges) in one PostgreSQL transaction.
2. **Initial state** is chosen: `WAITING` (has unmet dependencies), `SCHEDULED` (`run_at` in the future), or `QUEUED` (runnable now).
3. **Release.** When dependencies succeed (`WAITING`→…) or `run_at` arrives (`SCHEDULED`→`QUEUED`), the job is published to the Redis ready queue.
4. **Claim.** A worker pulls a job. In one PostgreSQL transaction the job moves `QUEUED`→`LEASED`, a lease with a fencing token is created, and a `job_attempts` row is written.
5. **Run.** Worker moves the job `LEASED`→`RUNNING` and executes the handler, heartbeating to keep the lease alive.
6. **Complete.** Worker reports the result with its fencing token. Success → `SUCCEEDED`. Failure → Retry Manager decides: `SCHEDULED` (retry after backoff) or `DEAD` (attempts exhausted / non-retryable).
7. **Propagate.** On `SUCCEEDED`, the DAG engine releases ready children. On terminal failure, it marks descendants `UPSTREAM_FAILED`.

## 5. Job State Machine

States: `WAITING`, `SCHEDULED`, `QUEUED`, `LEASED`, `RUNNING`, `SUCCEEDED`, `DEAD`, `CANCELLED`, `UPSTREAM_FAILED`.
Terminal states: `SUCCEEDED`, `DEAD`, `CANCELLED`, `UPSTREAM_FAILED`.

| From | To | Trigger |
|------|----|---------|
| (new) | `WAITING` | Submitted with unmet dependencies |
| (new) | `SCHEDULED` | Submitted with future `run_at` |
| (new) | `QUEUED` | Submitted and runnable |
| `WAITING` | `SCHEDULED` / `QUEUED` | All dependencies succeeded (`SCHEDULED` if `run_at` still future) |
| `WAITING` | `UPSTREAM_FAILED` | A dependency reached a failure terminal state |
| `SCHEDULED` | `QUEUED` | `run_at` reached |
| `QUEUED` | `LEASED` | Worker claims job |
| `LEASED` | `RUNNING` | Worker starts execution |
| `LEASED` / `RUNNING` | `SUCCEEDED` | Worker reports success (valid fencing token) |
| `LEASED` / `RUNNING` | `SCHEDULED` | Attempt failed or lease/timeout expired, attempts remain (backoff applied) |
| `LEASED` / `RUNNING` | `DEAD` | Attempt failed/expired, attempts exhausted or non-retryable |
| `DEAD` | `QUEUED` | Manual requeue |
| `WAITING` / `SCHEDULED` / `QUEUED` / `LEASED` / `RUNNING` | `CANCELLED` | Cancel request |

Any transition not listed is illegal and is rejected by the single transition function. Each transition also writes a `job_events` row in the same transaction.

## 6. Scheduler Architecture

The Scheduler runs a periodic tick (configurable) that is safe to run on multiple instances: all mutations are guarded by PostgreSQL row locks (`FOR UPDATE SKIP LOCKED`) or atomic Redis scripts, so multiple schedulers cooperate rather than conflict.

Each tick:
1. **Promote delayed jobs** whose `run_at` is due (Redis Lua script, then PostgreSQL transition `SCHEDULED`→`QUEUED`).
2. **Release dependency-ready jobs** produced by the DAG engine.
3. **Sweep expired leases** (via Lease Manager).
4. **Reconcile**: periodically compare PostgreSQL `QUEUED`/`SCHEDULED` jobs with Redis and repair any divergence (re-enqueue missing, drop stale).
5. **Apply aging** to prevent starvation.

## 7. Priority Scheduling

- Priority levels (highest first): `CRITICAL`, `HIGH`, `NORMAL`, `LOW`.
- Order: priority, then earliest `run_at`, then submission sequence (FIFO).
- A Redis queue per priority level; dequeue scans from highest to lowest.
- **Aging:** jobs waiting beyond a threshold are effectively promoted one level so low priority cannot starve indefinitely.

## 8. Delayed Jobs

Delayed jobs (`run_at` in the future, and retry backoffs) live in the PostgreSQL row (`state = SCHEDULED`, `run_at`) as truth, and in a Redis sorted set scored by `run_at` for efficient polling. A Lua script atomically moves due members to the ready queue. If Redis is lost, reconciliation rebuilds the sorted set from PostgreSQL.

## 9. DAG / Dependency Execution

- Dependencies are stored as edges in `job_dependencies (job_id, depends_on_id)`.
- On submission the graph is validated: **cycles are rejected**, size limits enforced.
- Each `WAITING` job tracks `unmet_dependencies`. When a parent reaches `SUCCEEDED`, the counter is decremented in a transaction; the transaction that brings it to zero releases the job — **exactly once**, even if parents finish concurrently (guarded by row lock/conditional update).
- A parent reaching `DEAD`, `CANCELLED`, or `UPSTREAM_FAILED` marks dependents `UPSTREAM_FAILED`, recursively.
- Supported shapes: chains, fan-out, fan-in, diamonds.

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
- **Reclaim:** expired jobs are returned via the Retry Manager (`SCHEDULED`) or `DEAD`, counting as a failed attempt.

## 12. Retries, Timeouts, and Idempotency

- **Retry:** per-job `max_attempts`, exponential backoff with jitter, retryable/non-retryable classification. Retries are delayed jobs. Every attempt is stored in `job_attempts`.
- **Dead-letter:** exhausted jobs become `DEAD`, retained for inspection and manual requeue.
- **Timeouts:** each job has an execution timeout. The worker enforces it via cooperative cancellation; the lease sweeper enforces it as a backstop for hung or unreachable workers.
- **Idempotency (submission):** a unique `idempotency_key` guarantees duplicate submissions return the original job.
- **Idempotency (execution):** because delivery is at-least-once, handlers receive a stable `job_id` and `attempt` to dedupe external side effects. This contract is documented for handler authors.

## 13. PostgreSQL Responsibilities

Durable source of truth for: `jobs`, `job_dependencies`, `job_attempts`, `workers`, `job_events` (audit), leases and fencing tokens, idempotency keys, retry state. Provides transactions, unique/foreign-key/check constraints, optimistic concurrency (`version`), and `FOR UPDATE SKIP LOCKED` claiming. Schema changes ship as versioned migrations.

## 14. Redis Responsibilities

Redis is used for: ready queues per priority, the delayed-job sorted set, worker heartbeat TTL keys, short-lived distributed locks (e.g., sweeper/reconciler leader election), and small caches (e.g., queue stats). Redis never holds the only copy of any job. Loss or flush of Redis degrades latency until reconciliation completes, but never loses or duplicates committed work.

## 15. REST API Boundary

The API (`/api/v1`) is the only external interface. It performs authentication (API key), validation, and translation to core services; it never touches Redis queues or executes jobs directly. Operations: submit job, submit DAG, get/list/filter jobs, cancel, requeue dead, list workers, stats. Operational endpoints: `/healthz` (liveness), `/readyz` (dependencies reachable), `/metrics` (Prometheus format). Workers and scheduler interact with core services directly, not through the REST API.

## 16. Concurrency Model

- Multiple API, scheduler, and worker instances run concurrently.
- Cross-process coordination uses PostgreSQL row locks, conditional updates (`WHERE state = ... AND version = ...`), and atomic Redis Lua scripts. No in-process lock is relied on for cross-process correctness.
- In-process: Drogon event loops for I/O; a bounded thread pool for job execution; a connection pool for PostgreSQL; shared state guarded by well-scoped mutexes/atomics; verified under ThreadSanitizer.

## 17. Failure-Recovery Flow

| Failure | Recovery |
|---------|----------|
| Worker crashes mid-job | Heartbeats stop → lease expires → sweeper reclaims job → retry or `DEAD`. Fencing blocks any late write from the old worker. |
| Worker hangs | Execution timeout (cooperative) then lease expiry backstop. |
| Scheduler crashes | Stateless; another instance (or restart) resumes. Row locks/conditional updates prevent double-processing. |
| Redis lost/flushed | Reconciliation rebuilds queues and delayed set from PostgreSQL. |
| PostgreSQL unavailable | Writes fail fast, `/readyz` goes unready, workers stop claiming; in-flight jobs finish locally and report on reconnect (fencing validates). |
| API instance crashes | Clients retry using idempotency keys. |

## 18. Data Consistency Model

- Strong consistency for job state: all transitions are single PostgreSQL transactions.
- Redis is eventually consistent with PostgreSQL; divergence is bounded by the reconciliation interval and is always resolved in favor of PostgreSQL.
- Delivery guarantee: **at-least-once**. Duplicate execution is possible only after failure/timeout; duplicate **completion** is prevented by fencing and the state machine.
- Publish ordering: PostgreSQL commit before Redis publish. A crash between them is repaired by reconciliation.

## 19. Graceful Shutdown

On SIGTERM/SIGINT:
1. Mark `/readyz` unready; API stops accepting new requests, completes in-flight ones.
2. Scheduler finishes the current tick and stops.
3. Worker enters `DRAINING`: stops claiming, finishes in-flight jobs up to a deadline, reports results, releases leases of unstarted jobs, deregisters.
4. Jobs still running at the deadline are left to lease expiry (reclaimed by recovery).
5. Close pools and flush logs.

## 20. Observability

- **Logging:** spdlog structured logs with job, worker, and request correlation ids.
- **Metrics (Prometheus-compatible):** queue depth per priority, jobs by state, job latency and queue-wait histograms, worker counts by state, lease expirations, retries, dead-letter count, reconciliation repairs, API request rate/latency/errors.
- **Health:** `/healthz`, `/readyz`.
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
