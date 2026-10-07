# TaskFlow — Living Project Summary

> Updated after **every** completed phase. Only implemented **and validated** work is recorded as done. Authority order: `Phases.md` → `ARCHITECTURE.md` → `TECHSTACK.md` → `summary.md`.

## Project Overview

TaskFlow is a distributed job execution platform in C++20: REST API, priority scheduler, delayed jobs, DAG dependencies, concurrent worker pool, leasing/heartbeat-based failure recovery, retries, timeouts, and idempotency. PostgreSQL is the durable source of truth; Redis provides fast coordination. Repository: https://github.com/iamankoo/TaskFlow

## Current Status

**Phases 1 and 2 complete.** TaskFlow can durably create, read, update and track jobs in PostgreSQL: job model, strongly typed state machine (enforced in C++ and by a database trigger), attempts, results, audit events and an idempotency-key uniqueness foundation, all behind a transactional `JobRepository`. 177 tests pass (also under ASan + UBSan) against real PostgreSQL and Redis, and the Compose stack was built and validated in GitHub Codespaces.

There is still no scheduler, queue, worker or job API: the running server exposes only `GET /health`.

## Current Phase

None in progress. Phase 3 starts only on the owner's instruction.

## Completed Phases

- **Phase 1 — Foundation & Project Infrastructure.** Commit `75e2548` (`75e25489a2ddb47b9afb465ca8eb15dbeb147942`).
- **Phase 2 — Job Model & Persistent State.** Commit hash: reported in the Phase 2 completion report and recorded here by the first commit of Phase 3 (a commit cannot contain its own hash).

## Phase-by-Phase Summary

| # | Phase | Status | Commit |
|---|-------|--------|--------|
| 1 | Foundation & Project Infrastructure | **COMPLETE** | `75e2548` |
| 2 | Job Model & Persistent State | **COMPLETE** | see Git Commit History |
| 3 | Scheduler & Priority Queue | NOT STARTED | — |
| 4 | DAG Dependency Engine | NOT STARTED | — |
| 5 | Concurrent Worker Pool | NOT STARTED | — |
| 6 | Job Leasing, Heartbeats & Failure Recovery | NOT STARTED | — |
| 7 | Retries, Timeouts & Idempotency | NOT STARTED | — |
| 8 | REST API & Production Operations | NOT STARTED | — |
| 9 | Testing, Load Testing & Performance Engineering | NOT STARTED | — |
| 10 | Final Hardening, Benchmark & Release (FINAL) | NOT STARTED | — |

### Phase 1 (summary)

Build system (CMake presets, vcpkg manifest), configuration, logging, error types, PostgreSQL and Redis connection pools, migration runner, Drogon HTTP server with `GET /health`, graceful lifecycle, Docker/Compose, GitHub Actions CI. The Phase 1 CI run on GitHub finished **green on all three jobs** (Release build and tests, ASan build and tests, Docker image and Compose `/health`). Full detail is in the git history of this file at commit `75e2548`.

### Phase 2 detail

**Objective.** Make PostgreSQL the durable source of truth for jobs: create, persist, read, update state, track attempts, persist results, transactionally.

**Job model** (`include/taskflow/jobs/`)

- `Job`: `JobId` (strongly typed UUID), `sequence` (global submission order), `type`, `payload` and `metadata` (JSON objects), `JobPriority` (LOW, NORMAL, HIGH, CRITICAL), `JobState`, `scheduled_at`, `RetryPolicy` (`max_attempts`, `initial_delay`, `backoff_multiplier`, `max_delay`), per-attempt `timeout`, `attempt_count`, optional `idempotency_key`, `last_error`, `version`, `created_at`, `updated_at`, `finished_at`.
- `NewJob` (what a caller submits) and `JobUpdate` (the fields editable before a job runs) validate themselves: type charset/length, JSON-object payload (≤ 1 MiB) and metadata (≤ 64 KiB), timeout 1 ms–7 d, retry bounds, idempotency key shape, READY jobs may not be scheduled in the future.
- `JobAttempt`, `AttemptOutcome`, `JobResult`, `JobEvent`. Strong types and enums throughout; no free-form state strings in code.

**Job states and transitions** (single source of truth: `src/jobs/job_state.cpp`)

States: `PENDING`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `RETRY_WAIT`, `TIMEOUT`, `CANCELLED`. Exactly 13 transitions are legal:

`PENDING→READY|CANCELLED`, `READY→RUNNING|CANCELLED`, `RUNNING→COMPLETED|FAILED|RETRY_WAIT|TIMEOUT|CANCELLED`, `RETRY_WAIT→READY|CANCELLED`, `FAILED→READY` and `TIMEOUT→READY` (manual requeue). `COMPLETED` and `CANCELLED` are permanent. Everything else (e.g. `COMPLETED→RUNNING`, `CANCELLED→RUNNING`, `PENDING→RUNNING`) is rejected with `InvalidTransitionError`.

The same table is enforced by a PostgreSQL trigger; an integration test compares C++ and database for all 56 ordered pairs of distinct states. The repository additionally refuses transitions that need bookkeeping: `READY→RUNNING` only via `CreateAttempt`, leaving `RUNNING` (except cancel) only via `StoreResult`.

**PostgreSQL schema** (`migrations/0001`–`0004`, documented in `docs/DATABASE.md`)

- `jobs`: UUID PK, identity `sequence`, `jsonb` payload/metadata, 8-value status CHECK, priority 0–3, retry/timeout columns with CHECKs, `UNIQUE (idempotency_key)`, optimistic `version`, `finished_at` set exactly in final states (CHECK), immutable-column guard, version/updated_at trigger.
- `job_attempts`: FK to jobs (cascade), `UNIQUE (job_id, attempt_number)`, status CHECK, `finished_at` ⇔ not RUNNING, **at most one RUNNING attempt per job** (partial unique index).
- `job_results`: one per attempt, composite FK `(attempt_id, job_id)` so a result cannot name another job's attempt, success/failure shape CHECK, read independently of the job.
- `job_events`: append-only audit trail (update trigger blocks edits); written in the same transaction as each change.
- Indexes (and the ones deliberately not created) are explained in `docs/DATABASE.md`: `idx_jobs_ready_queue` (partial, READY, `priority DESC, scheduled_at, sequence`), `idx_jobs_due` (partial, PENDING/RETRY_WAIT), `idx_jobs_created_at`, plus constraint-backed indexes. Two tests assert the planner actually uses the first two for the Phase 3 queries they are designed for (no sort needed).

**Repository** (`include/taskflow/db/job_repository.hpp`, all SQL isolated in `src/db/job_repository.cpp`): `CreateJob`, `GetJob`, `FindJobByIdempotencyKey`, `UpdateJobState`, `UpdateJob`, `CreateAttempt`, `GetAttempts`, `StoreResult`, `GetResult`, `GetResults`, `GetEvents`. Row locking (`SELECT … FOR UPDATE`) serializes writers; optimistic `expected_state`/`expected_version` guards stale writers. Database errors map onto `ValidationError`, `ConflictError`, `NotFoundError`, `InvalidTransitionError`.

**Transactions** (RAII `PgTransaction`; pooled connections are returned to idle, with a rollback, before reuse)

- `CreateJob`: job row + creation event.
- `CreateAttempt`: `READY→RUNNING` + `attempt_count` increment + attempt row + event.
- `StoreResult`: result row + attempt closed + job moved to its next state + event.
- `UpdateJobState` cancelling a RUNNING job: attempt closed as CANCELLED + job CANCELLED + event.
- Rollback is proven by injecting a failure (a test-only trigger) at the last write of each of these and checking nothing earlier persisted.

**Idempotency foundation (storage only):** unique key column, `ConflictError` on duplicates, `FindJobByIdempotencyKey`. A test with 8 concurrent submissions of one key yields exactly one job. The execution-time algorithm is Phase 7.

**Results:** success (optional JSON output) and failure (error message; timed-out and cancelled attempts included), with completion time and attempt association, retrievable on their own (`GetResult` = latest, `GetResults` = all).

**API boundary:** no job REST API (Phase 8). `JobRepository` plus the domain types are the application interface Phase 8 will expose. `/health` is unchanged and still passes its tests.

## Implemented Features

- Everything from Phase 1 (build, config, logging, pools, migration runner, `/health`, lifecycle, Docker, CI).
- Persistent job model, state machine (C++ and database), attempts, results, audit events, idempotency-key storage, transactional repository, schema docs.

## Architecture Summary

As in `ARCHITECTURE.md`. Implemented so far: the infrastructure from Phase 1 and the persistent job layer from Phase 2 (jobs, attempts, results, events, state machine). Scheduler, queues, DAG engine, retry manager, lease manager, worker manager and worker pool do not exist yet.

## Technology Stack

Locked (see `TECHSTACK.md`): C++20, CMake, Drogon, PostgreSQL, Redis, Docker, Docker Compose, GoogleTest, spdlog, JSON, Prometheus-compatible metrics, GitHub Actions. Package manager: vcpkg (manifest mode, baseline `4b308f5`). No new dependencies in Phase 2. PostgreSQL 13+ required (`gen_random_uuid()`); tested on 16.

## Database Summary

Tables: `schema_migrations`, `jobs`, `job_attempts`, `job_results`, `job_events` (4 migrations). Constraints, triggers and indexes as above and in `docs/DATABASE.md`. `job_dependencies` is added in Phase 4 and `workers` in Phase 5, together with the code that uses them.

## API Summary

| Endpoint | Behavior |
|---|---|
| `GET /health` | Unchanged from Phase 1: `200` with dependency status, or `503` `degraded`/`shutting_down`. |

No job endpoints yet (Phase 8).

## Testing Summary

**177 tests, all passing**, run against real PostgreSQL 16 and Redis 7 (no database mocks):

| Suite | Total | New in Phase 2 | Covers (Phase 2) |
|---|---|---|---|
| `taskflow_unit_tests` | 73 | 34 | all 8×8 state pairs against an independent expected table, terminal/initial states, name round-trips, `NewJob`/`RetryPolicy`/`JobUpdate`/`AttemptOutcome` validation, `JobId`, priorities |
| `taskflow_integration_tests` | 97 | 68 | migrations (fresh database, re-run no-op, naming), every CHECK/UNIQUE/FK constraint, triggers (initial state, transitions, immutability, append-only), C++↔database transition parity (56 pairs), index use by the planner, cascade delete, repository CRUD/state/attempt/result behavior, optimistic concurrency, concurrent claim and concurrent idempotent submission, transaction commit and rollback with injected failures, pool recovery after an open transaction |
| `taskflow_http_tests` | 7 | 0 | `/health` end to end (unchanged) |

Results, all in GitHub Codespaces:

- Release build with `-Werror` (the `ci` preset): **177/177 pass**, 0 skipped.
- ASan + UBSan build: **177/177 pass**, 0 skipped, no sanitizer reports.
- Not run: ThreadSanitizer (first needed in Phase 5), clang-tidy, native Windows build.

**Docker/Codespaces validation (live stack, GitHub Codespace `standardLinux32gb`: 4 vCPU, 16 GB, Docker 29.8.0, Compose v5.5.1):**

- Test and runtime images built; `docker compose up --wait` brought `postgres`, `redis` and `taskflow` to healthy.
- TaskFlow's startup log shows `applying migration 0001…0004`, `migrations complete: 4 applied`; `GET /health` returned `200` with both dependencies up.
- In the running database: tables `jobs`, `job_attempts`, `job_results`, `job_events`, `schema_migrations`; `schema_migrations` lists versions 1–4; all 15 expected indexes and the three triggers exist.
- Persistence: a job and attempt written to the running database survived restarting PostgreSQL and TaskFlow (`RUNNING`, `attempt_count 1`, `version 2`). After the restart the log shows `migrations complete: 0 applied`, `taskflow --migrate-only` also applied 0, and `schema_migrations` still had 4 rows.
- The running database itself rejected an illegal transition (`illegal job transition RUNNING -> PENDING`) and a duplicate idempotency key (`uq_jobs_idempotency_key`).

## Performance Benchmarks

None measured. Phase 2 indexes are validated for planner use on 4,000 synthetic rows, not for throughput. No performance claims are made until measured (Phases 9–10).

## Known Issues

1. **Requeued jobs keep their attempt count.** `FAILED`/`TIMEOUT → READY` does not reset `attempt_count`; a requeued job with no attempts left fails `CreateAttempt` with `ConflictError` until its retry policy is edited. This is deliberate and documented (`docs/DATABASE.md`); Phase 7 decides the manual-requeue policy.
2. **Legal transition table will gain edges.** Phase 4 must add `PENDING→FAILED` (upstream failure). Phase 6 may add a reclaim edge out of `RUNNING`. Each change must update the C++ table, the trigger (in a new migration) and the parity test together.
3. **Audit completeness depends on the repository.** The database guarantees only valid transitions and an append-only event table; an event is written by `JobRepository`, so a manual `psql` update changes state without an event row.
4. **JSON `null` output is stored as SQL `NULL`** and reads back as "no output".
5. **`/health` latency is not bounded** when a dependency hostname stops resolving (seen as ~12 s in Phase 1 validation); a per-probe deadline is planned for Phase 8.
6. **One unreproduced test-startup flake from Phase 1** (a single HTTP-test startup under ASan). Fixture now fails loudly; it did not recur in the final Phase 2 runs (full suite, ASan/UBSan suite and the live-stack checks).
7. **`ctest` counts a skipped test as a pass.** The only conditional skip (`PostgresTest.WrongPasswordIsRejected` under trust authentication) did not skip in any run; the full-suite checks above also searched for skips.
8. Linux only. The Codespace's Docker needed `net.bridge.bridge-nf-call-iptables=0` (a stale legacy-iptables `FORWARD DROP` rule set blocked traffic between containers on one bridge); this is a Codespace environment quirk, not a project requirement.
9. Local Windows Docker was not used for Phase 2 validation, per the Codespaces rule. The local machine's clone is kept in sync through GitHub.

## Important Decisions

| Date | Decision |
|------|----------|
| 2026-10-07 | Locked 10-phase roadmap, architecture, and tech stack. |
| 2026-10-07 | PostgreSQL = durable source of truth; Redis = rebuildable coordination layer. |
| 2026-10-07 | Single binary with role flags (`api`, `scheduler`, `worker`, `all`); roles arrive with their phases. |
| 2026-10-07 | At-least-once delivery with fencing tokens; exactly-once is not claimed. |
| 2026-10-07 | All commits/pushes use `iamankoo <aniketraj00384@gmail.com>` only; no AI attribution. |
| 2026-10-07 | vcpkg (manifest mode, pinned baseline) is the dependency manager. |
| 2026-10-07 | libpq and hiredis used directly through small pooled wrappers; Drogon for HTTP only. |
| 2026-10-07 | Single `/health` endpoint replaces `/healthz` + `/readyz`. |
| 2026-10-07 | Signals handled on a `sigwait` thread, not in Drogon's signal handlers. |
| 2026-10-07 | Integration tests fail (not skip) when PostgreSQL/Redis are unreachable. |
| 2026-10-07 | **State model changed (owner instruction for Phase 2):** the architecture's nine states (`WAITING`, `SCHEDULED`, `QUEUED`, `LEASED`, `RUNNING`, `SUCCEEDED`, `DEAD`, `CANCELLED`, `UPSTREAM_FAILED`) are replaced by the owner's eight (`PENDING`, `READY`, `RUNNING`, `COMPLETED`, `FAILED`, `RETRY_WAIT`, `TIMEOUT`, `CANCELLED`). Mapping: WAITING/SCHEDULED → PENDING; QUEUED → READY; LEASED+RUNNING → RUNNING (the lease becomes separate data in Phase 6); SUCCEEDED → COMPLETED; DEAD → FAILED/TIMEOUT; retry backoff → RETRY_WAIT; UPSTREAM_FAILED → handled by DAG policy in Phase 4. `ARCHITECTURE.md` and `Phases.md` rewritten to match. |
| 2026-10-07 | **Table scope:** `job_dependencies` moves to Phase 4 and `workers` to Phase 5 (each arrives with its code). Phase 2 adds `job_results` and keeps `job_events` (required by the architecture's audit-trail rule). `Phases.md` updated. |
| 2026-10-07 | Status stored as `text` + CHECK (not a PostgreSQL enum) so later phases can extend it with an ordinary migration; priority as `smallint` 0–3 so a descending sort is scheduling order. |
| 2026-10-07 | The state machine is enforced twice (C++ and trigger) with a parity test, and `READY→RUNNING` / leaving `RUNNING` are only reachable through `CreateAttempt` / `StoreResult`, so no repository call can produce a state without its attempt bookkeeping. |
| 2026-10-07 | The one-active-attempt-per-job rule and the result↔attempt↔job consistency are database constraints (partial unique index, composite FK), not just code. |
| 2026-10-07 | Docker validation for Phase 2 ran in GitHub Codespaces (4 vCPU / 16 GB), not on the local Windows Docker, per the owner's rule. |

## Git Commit History

| Commit | Message | Phase |
|--------|---------|-------|
| `585b4b5` | docs: initialize TaskFlow architecture and development roadmap | Initialization |
| `75e2548` | feat: complete TaskFlow phase 1 foundation | Phase 1 |
| _next commit on `main`_ | feat: complete TaskFlow phase 2 job persistence | Phase 2 |

The Phase 2 commit hash is reported in the Phase 2 completion report and written into this table by the first commit of Phase 3.

## Next Phase

**Phase 3 — Scheduler & Priority Queue** (NOT STARTED; begins only on the owner's instruction).
