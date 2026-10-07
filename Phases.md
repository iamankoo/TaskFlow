# TaskFlow — Master Development Roadmap

**TaskFlow** is a distributed job execution platform written in C++20. It accepts jobs over a REST API, schedules them by priority and time, resolves DAG dependencies, executes them on a concurrent worker pool, and recovers from worker failure using leases and heartbeats. PostgreSQL is the durable source of truth; Redis provides fast coordination.

## Document Hierarchy (locked)

```
Phases.md → ARCHITECTURE.md → TECHSTACK.md → summary.md → current phase implementation
```

If documents conflict, stop and resolve the conflict before implementing. Architecture, stack, and phase scope are never changed silently; a change requires an explicit, recorded decision in `summary.md` ("Important Decisions") and an update to every affected document.

## Roadmap Rules

1. There are **exactly 10 phases**. **Phase 10 is the final phase.** There is no Phase 11.
2. Phases are executed **strictly one at a time, in order**. Work for a future phase must not begin until that phase is officially started by the owner.
3. A phase exits only when **every** completion criterion is met and verified. No partial exits.
4. At the end of every phase: update `summary.md`, commit, and push (see Git Workflow).
5. Nothing is claimed as implemented in `summary.md` unless it is implemented **and** validated.

## Git Workflow (locked)

- Identity for every commit and push: `iamankoo <aniketraj00384@gmail.com>` (author and committer). No other identity, no co-author or AI attribution trailers.
- Verify `git config user.name` / `git config user.email` before the first commit of each session.
- One phase = one or more clean commits, ending with a `summary.md` update that records the phase commit hash.
- Commit messages use Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `build:`, `ci:`, `chore:`).

## Phase Overview

| # | Phase | Status |
|---|-------|--------|
| 1 | Foundation & Project Infrastructure | Not started |
| 2 | Job Model & Persistent State | Not started |
| 3 | Scheduler & Priority Queue | Not started |
| 4 | DAG Dependency Engine | Not started |
| 5 | Concurrent Worker Pool | Not started |
| 6 | Job Leasing, Heartbeats & Failure Recovery | Not started |
| 7 | Retries, Timeouts & Idempotency | Not started |
| 8 | REST API & Production Operations | Not started |
| 9 | Testing, Load Testing & Performance Engineering | Not started |
| 10 | Final Hardening, Benchmark & Release (FINAL) | Not started |

---

## Phase 1 — Foundation & Project Infrastructure

**Objective.** Establish a reproducible build, test, container, and CI baseline so every later phase lands on solid infrastructure.

**Scope.** Repository layout, CMake build, dependency management, logging, configuration, Docker/Compose environment with PostgreSQL and Redis, CI. No job logic.

**Implementation tasks.**
- Repository layout: `src/`, `include/taskflow/`, `tests/`, `migrations/`, `docker/`, `docs/`, `.github/workflows/`.
- Top-level `CMakeLists.txt` targeting C++20 with warnings-as-errors for project code, sanitizer options (ASan/UBSan/TSan), and a library + executable + test target split.
- Dependency acquisition (vcpkg or Conan manifest, chosen and recorded in `summary.md`) for Drogon, spdlog, GoogleTest, JSON library, PostgreSQL client, Redis client.
- Configuration loader (file + environment overrides) with validation.
- spdlog-based structured logging wrapper.
- Connectivity layer skeletons: PostgreSQL connection pool and Redis client with health checks (connect/ping only).
- Multi-stage `Dockerfile`; `docker-compose.yml` with `taskflow`, `postgres`, `redis`.
- `.clang-format`, `.clang-tidy`, `.gitignore`, `.dockerignore`, `.env.example`.
- GitHub Actions: configure, build, test, and sanitizer jobs.
- Minimal `GET /health` endpoint proving the Drogon wiring: reports application availability and PostgreSQL/Redis connectivity (`200` healthy, `503` degraded or shutting down). No job API.

**Expected deliverables.** Building project skeleton, working Compose stack, passing CI, README with build/run instructions.

**Tests/validation.** GoogleTest smoke test and config-loader unit tests; Compose stack starts and health checks pass; CI green on a clean checkout.

**Completion criteria.**
- Clean-clone build succeeds locally and in CI.
- `docker compose up` brings up all three services healthy.
- Unit tests pass; sanitizer build passes.
- `summary.md` updated and phase commit pushed.

**Dependencies.** None.

---

## Phase 2 — Job Model & Persistent State

**Objective.** Define the job domain model and make PostgreSQL the durable source of truth for all job state.

**Scope.** Schema, migrations, repositories, state machine enforcement. No scheduling or execution.

**Implementation tasks.**
- Domain types: `Job`, `JobState`, `JobPriority`, `JobDependency`, `Worker`, `JobAttempt`, `JobEvent`.
- Job state machine (as defined in `ARCHITECTURE.md`) with a single transition function that rejects illegal transitions.
- Migration tooling and versioned SQL migrations for `jobs`, `job_dependencies`, `job_attempts`, `workers`, `job_events`, and supporting indexes.
- Repository layer with transactional operations, optimistic concurrency (`version` column), and `SELECT ... FOR UPDATE SKIP LOCKED` primitives.
- Idempotency key column with unique constraint (storage only; semantics completed in Phase 7).
- Append-only audit trail in `job_events` written in the same transaction as each transition.

**Expected deliverables.** Migrations, repository library, state-machine module, schema documentation in `docs/`.

**Tests/validation.** Unit tests for every legal and illegal transition; integration tests against real PostgreSQL covering CRUD, concurrent transition races, and constraint enforcement.

**Completion criteria.**
- All migrations apply cleanly forward on an empty database.
- Illegal transitions are rejected in code and cannot be produced via repository API.
- Concurrency race tests pass reliably.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phase 1.

---

## Phase 3 — Scheduler & Priority Queue

**Objective.** Select the next runnable job correctly by priority and time, including delayed jobs.

**Scope.** Scheduler loop, Redis-backed ready queue and delayed set, PostgreSQL reconciliation. Jobs are dequeued but not executed by workers yet (handoff interface only).

**Implementation tasks.**
- Priority levels and ordering rules (priority, then `run_at`, then FIFO via sequence).
- Redis ready queue per priority and a Redis sorted set for delayed jobs scored by `run_at`.
- Delayed-job promoter moving due jobs to the ready queue atomically (Lua script).
- Enqueue path: persist to PostgreSQL first, then publish to Redis.
- Dequeue path: atomic claim with PostgreSQL confirming state transition.
- Reconciliation loop that rebuilds Redis queues from PostgreSQL after Redis loss or divergence.
- Starvation protection (aging) for low-priority jobs.
- Scheduler tick configuration and metrics hooks.

**Expected deliverables.** Scheduler module, queue module, Lua scripts, reconciliation job.

**Tests/validation.** Ordering unit tests; delayed-job timing tests; integration tests with Redis; test that flushing Redis loses no jobs after reconciliation; starvation test.

**Completion criteria.**
- Jobs dequeue in the documented order under concurrent producers/consumers.
- Delayed jobs never run before `run_at` and run within the configured tolerance after.
- Full Redis loss is recovered with zero job loss.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–2.

---

## Phase 4 — DAG Dependency Engine

**Objective.** Support jobs that depend on other jobs, releasing them only when dependencies are satisfied.

**Scope.** Dependency graph storage, validation, resolution, and failure propagation.

**Implementation tasks.**
- Dependency submission with cycle detection (reject cyclic graphs at submit time).
- `WAITING` state handling: track unmet dependency count per job.
- On dependency success, atomically decrement counters and release newly-ready jobs to the scheduler.
- Failure policy: a failed or cancelled upstream marks downstream jobs `UPSTREAM_FAILED` (or per-job configured policy), recursively.
- Support for fan-out, fan-in, and diamond graphs; graph size limits.
- Graph inspection queries (ancestors, descendants, topological view).

**Expected deliverables.** DAG engine module, additional migrations if required, graph query API at the service layer.

**Tests/validation.** Cycle-detection tests; fan-in/fan-out/diamond execution-order tests; failure-propagation tests; concurrent completion races (two parents finishing simultaneously release the child exactly once).

**Completion criteria.**
- Cycles are always rejected.
- Child jobs run exactly once, only after all dependencies succeed.
- Failure propagation is correct and complete.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–3.

---

## Phase 5 — Concurrent Worker Pool

**Objective.** Execute jobs concurrently on a managed pool of workers.

**Scope.** Worker registration, pull-based job acquisition, handler dispatch, result reporting. Lease and failure handling are deferred to Phase 6.

**Implementation tasks.**
- Worker identity, registration, and deregistration recorded in PostgreSQL.
- Worker manager tracking capacity and status.
- Thread-pool executor with configurable concurrency and bounded internal queue (backpressure).
- Job handler registry (job `type` → handler function).
- Pull-based acquisition from the scheduler, honoring per-worker capacity.
- Result reporting: success/failure persisted via the state machine.
- Graceful shutdown: stop acquiring, drain in-flight jobs within a deadline.
- Built-in demonstration handlers (e.g., echo, sleep, fail) for testing.

**Expected deliverables.** Worker library and process, handler registry, shutdown logic.

**Tests/validation.** Concurrency tests with many jobs and workers (run under TSan); no job executed twice under normal operation; graceful-shutdown test; backpressure test.

**Completion criteria.**
- Configured concurrency is respected and never exceeded.
- Normal-operation execution is exactly-once-observed (no duplicates without failure).
- TSan-clean.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–4.

---

## Phase 6 — Job Leasing, Heartbeats & Failure Recovery

**Objective.** Make execution safe when workers crash, hang, or lose connectivity.

**Scope.** Lease acquisition, renewal, expiry, worker liveness, and reclamation of orphaned jobs.

**Implementation tasks.**
- Lease on every claimed job (`lease_owner`, `lease_expires_at`, `lease_token`) persisted in PostgreSQL.
- Lease renewal tied to worker heartbeats; fencing token checked on every state write so stale workers cannot overwrite results.
- Worker heartbeat protocol and liveness tracking (Redis TTL keys for fast detection, PostgreSQL for durable record).
- Failure detector (lease sweeper) that finds expired leases and returns jobs to `QUEUED` (or failure path) atomically.
- Worker states: `ACTIVE`, `DRAINING`, `SUSPECT`, `DEAD`.
- Recovery behavior defined for scheduler restart, worker restart, Redis loss, and PostgreSQL reconnect.

**Expected deliverables.** Lease manager, heartbeat service, failure detector, recovery documentation.

**Tests/validation.** Kill-worker-mid-job test; network-partition simulation; stale-worker (zombie) write rejected via fencing; sweeper correctness under concurrency; chaos-style integration tests in Compose.

**Completion criteria.**
- A job on a killed worker is reclaimed within the documented bound (lease TTL + sweep interval).
- A zombie worker can never overwrite a newer attempt's state.
- No job is lost across worker, scheduler, or Redis restarts.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–5.

---

## Phase 7 — Retries, Timeouts & Idempotency

**Objective.** Provide predictable at-least-once execution with controls that make duplicates safe and bounded.

**Scope.** Retry policy, backoff, timeouts, dead-letter handling, and idempotent submission/execution.

**Implementation tasks.**
- Per-job retry policy: `max_attempts`, exponential backoff with jitter, retryable vs. non-retryable errors.
- Retries re-enter the scheduler as delayed jobs; each attempt recorded in `job_attempts`.
- Execution timeout enforced by the worker with cooperative cancellation, and enforced by the lease sweeper as a backstop.
- Dead-letter state (`DEAD`) after exhausted retries, with manual requeue.
- Idempotent submission: duplicate `idempotency_key` returns the existing job.
- Idempotency support for handlers: stable `job_id` and `attempt` passed to handlers; documented at-least-once contract.
- Cancellation of queued and running jobs.

**Expected deliverables.** Retry manager, timeout enforcement, dead-letter handling, idempotency module.

**Tests/validation.** Backoff schedule tests; timeout tests (cooperative and forced); duplicate submission under concurrency; retry-exhaustion path; cancellation tests.

**Completion criteria.**
- Retry timing follows the configured schedule within tolerance.
- Duplicate submissions with the same key produce exactly one job.
- Hung jobs are always terminated or reclaimed.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–6.

---

## Phase 8 — REST API & Production Operations

**Objective.** Expose TaskFlow through a complete, documented REST API and make it operable in production.

**Scope.** Drogon REST API, validation, authentication, observability, deployment.

**Implementation tasks.**
- Endpoints: submit job, submit DAG, get job, list/filter jobs, cancel job, requeue dead job, list workers, queue/system stats.
- Request validation, consistent error model, pagination, and API versioning (`/api/v1`).
- API-key authentication and request size limits.
- OpenAPI specification in `docs/`.
- `/health` (introduced in Phase 1) and Prometheus-compatible `/metrics` (queue depth, job state counts, latency histograms, worker counts, lease expirations, retries).
- Structured request/job-correlated logging.
- Production Docker images and Compose profile (separate `api`, `scheduler`, `worker` roles or a single binary with role flags, as defined in `ARCHITECTURE.md`).
- Graceful shutdown across all roles.

**Expected deliverables.** Full REST API, OpenAPI spec, metrics endpoint, production Compose configuration, operations guide.

**Tests/validation.** API integration tests for every endpoint including error paths; auth tests; metrics scrape test; end-to-end test from HTTP submit to completed job.

**Completion criteria.**
- Every documented endpoint is implemented and tested.
- A job can be submitted, executed, and observed end-to-end through the API only.
- Metrics exposed and verified by a Prometheus scrape.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–7.

---

## Phase 9 — Testing, Load Testing & Performance Engineering

**Objective.** Prove correctness at scale and find and fix performance bottlenecks with measurements.

**Scope.** Test-suite completion, load and soak testing, profiling, optimization.

**Implementation tasks.**
- Fill coverage gaps; add coverage reporting to CI.
- Property/stress tests for the state machine, scheduler ordering, and DAG engine.
- Fault-injection suite: worker kill, Redis restart, PostgreSQL restart, network delay.
- Load-test harness (submit rate, worker count, job duration, DAG shapes) with reproducible configs.
- Profiling (CPU, locks, allocations), query plan review (`EXPLAIN ANALYZE`), index and connection-pool tuning.
- Baseline and post-optimization measurements recorded in `docs/benchmarks/`.

**Expected deliverables.** Complete test suite, load-test tooling, benchmark reports, documented optimizations.

**Tests/validation.** Full suite green in CI including sanitizers; soak test (extended run) with no leaks or job loss; fault-injection suite passes.

**Completion criteria.**
- Zero lost jobs and zero unintended duplicate completions across the fault-injection and soak runs.
- Throughput and latency numbers measured, reproducible, and recorded (no unmeasured claims).
- Identified bottlenecks addressed or documented.
- `summary.md` updated and phase commit pushed.

**Dependencies.** Phases 1–8.

---

## Phase 10 — Final Hardening, Benchmark & Release (FINAL PHASE)

**Objective.** Final release and validation phase: harden, benchmark officially, document, and ship `v1.0.0`. **This is the last phase of the project.**

**Scope.** Security and robustness hardening, final benchmark, documentation completion, release. No new features.

**Implementation tasks.**
- Security review: input validation, auth, dependency audit, container hardening (non-root, minimal image).
- Robustness review: resource limits, timeouts on all I/O, behavior under dependency outage.
- Final full-system benchmark on documented hardware; results published in `docs/benchmarks/` and `summary.md`.
- Complete `README.md`, operations runbook, API docs, and architecture diagrams verified against the code.
- Reconcile `Phases.md`, `ARCHITECTURE.md`, `TECHSTACK.md`, and `summary.md` against the final implementation.
- Release: tag `v1.0.0`, publish release notes, final CI run.

**Expected deliverables.** Release `v1.0.0`, final benchmark report, final documentation set, release notes.

**Tests/validation.** Entire test suite, sanitizer builds, fault-injection, soak, and final benchmark all green from a clean clone using the documented instructions.

**Completion criteria.**
- All documented features are implemented and validated; all documents match reality.
- Final benchmark recorded with methodology and hardware.
- Tag `v1.0.0` pushed; `summary.md` marked complete with known limitations listed honestly.

**Dependencies.** Phases 1–9.
