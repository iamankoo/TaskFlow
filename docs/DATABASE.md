# TaskFlow database schema (through Phase 4)

PostgreSQL is the durable source of truth (`ARCHITECTURE.md`). Everything below is created by the versioned migrations in `migrations/` and verified by `tests/integration/schema_test.cpp` and `tests/integration/job_repository_test.cpp`. Requires PostgreSQL 13+ (`gen_random_uuid()` is built in); developed and tested on 16.

```
jobs 1 ──< job_attempts 1 ──0..1 job_results
 │                                     │
 ├──< job_events                       └── (job_id, attempt_id) must match the attempt's job
 └──< job_results
```

Deleting a job cascades to its attempts, results and events.

## `jobs` (0001)

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK | `gen_random_uuid()`; immutable |
| `sequence` | `bigint` identity, unique | global submission order, the FIFO tie-breaker for the scheduler; cannot be set by clients |
| `type` | `text` | 1–255 chars; names the handler; immutable |
| `payload`, `metadata` | `jsonb` | must be JSON objects (CHECK) |
| `priority` | `smallint` | 0 LOW, 1 NORMAL, 2 HIGH, 3 CRITICAL; descending sort = scheduling order |
| `status` | `text` | one of the eight states (CHECK) |
| `scheduled_at` | `timestamptz` | earliest run time; used for delayed jobs and retry backoff |
| `max_attempts`, `attempt_count` | `integer` | `1..1000`; `0 <= attempt_count <= max_attempts` |
| `retry_initial_delay_ms`, `retry_backoff_multiplier`, `retry_max_delay_ms` | `integer`, `double precision`, `integer` | the retry policy (applied in Phase 7); `max >= initial`, multiplier `1..100` |
| `timeout_ms` | `integer` | per-attempt execution timeout, `> 0` (enforced in Phase 7) |
| `idempotency_key` | `text`, unique | nullable; NULLs never collide; immutable. Storage only in Phase 2 |
| `last_error` | `text` | message of the most recent failed attempt; cleared on success |
| `version` | `bigint` | optimistic-concurrency token, incremented by trigger on every update |
| `created_at`, `updated_at`, `finished_at` | `timestamptz` | `updated_at >= created_at`; `finished_at` is set **exactly** while the status is `COMPLETED`/`FAILED`/`TIMEOUT`/`CANCELLED` (CHECK) |

Triggers:

- `trg_jobs_initial_state` (insert): a job is created `PENDING` or `READY`.
- `trg_jobs_update_guard` (update): rejects any state change outside the 14-edge transition table (SQLSTATE `TF001`; `PENDING`→`FAILED` was added in migration 0005), refuses `READY` while a prerequisite is not `COMPLETED` (`TF004`), rejects edits of immutable columns (`TF002`), and maintains `version` and `updated_at`. The table is identical to `src/jobs/job_state.cpp`; a test compares both for every pair of states.

## `job_attempts` (0002)

One row per execution attempt: `attempt_number` (1-based, unique per job), `status` (`RUNNING`, `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `CANCELLED`), `started_at`, `finished_at` (set exactly when not `RUNNING`), `error_message` (only on unsuccessful finished attempts). A partial unique index allows **at most one `RUNNING` attempt per job**, whoever writes. `UNIQUE (id, job_id)` exists so `job_results` can reference the pair.

## `job_results` (0003)

The outcome of one attempt, kept apart from `jobs` so large outputs never bloat the hot table and a result can be read on its own. At most one per attempt (`UNIQUE (attempt_id)`); the composite foreign key `(attempt_id, job_id)` guarantees the attempt belongs to the same job. CHECK: success has `output` (optional) and no error; failure has an error and no output. A JSON `null` output is stored as SQL `NULL`.

## `job_events` (0004)

Append-only audit trail: `from_status` (NULL for the creation event), `to_status`, `reason`, `created_at`. Updates are rejected by trigger (`TF003`); deletes are allowed only so that deleting a job removes its history. Rows are written by `JobRepository` in the same transaction as the change they record.

## Indexes and why

| Index | Definition | Serves |
|---|---|---|
| `pk_jobs`, `uq_jobs_sequence`, `uq_jobs_idempotency_key` | constraints | lookup by id; submission order; idempotent submission lookup/uniqueness |
| `idx_jobs_ready_queue` | `(priority DESC, scheduled_at, sequence) WHERE status = 'READY'` | Phase 3 dequeue (`ORDER BY priority DESC, scheduled_at, sequence LIMIT n` is answered from the index with no sort) and rebuilding Redis after a flush. Partial, so it only costs writes for jobs that are currently runnable |
| `idx_jobs_due` | `(scheduled_at) WHERE status IN ('PENDING','RETRY_WAIT')` | promoting delayed and backed-off jobs whose time has come |
| `idx_jobs_created_at` | `(created_at DESC, id)` | newest-first listing for the Phase 8 API |
| `uq_job_attempts_job_number` | `(job_id, attempt_number)` | attempts of a job in order (also the foreign-key lookup) |
| `uq_job_attempts_one_running` | `(job_id) WHERE status = 'RUNNING'` | the one-active-attempt invariant |
| `idx_job_results_job` | `(job_id, id DESC)` | latest result of a job; foreign-key lookup |
| `idx_job_events_job` | `(job_id, id)` | a job's history; foreign-key lookup |

Deliberately **not** created, to avoid write cost with no demonstrated query: a plain `status` index (low cardinality; the partial indexes above cover the queries that matter, and per-state counts are an aggregate for Phase 8/9), an index on `type`, and a `RUNNING`-job index (Phase 6 adds one together with the lease columns it will query). Phase 9 re-evaluates every index against measured query plans.

Two tests assert the two scheduler-facing indexes are actually chosen by the planner for the queries they are designed for.

## Dependencies (Phase 4, migration 0005)

```
jobs 1 ──< job_dependencies >── 1 jobs      (job_id waits for depends_on_id)
```

`job_dependencies (job_id, depends_on_id, created_at)`

| Rule | Mechanism |
|---|---|
| both ends must be jobs | foreign keys `fk_job_dependencies_job`, `fk_job_dependencies_prerequisite` |
| no self-dependency | `ck_job_dependencies_no_self` |
| no duplicate edge | primary key `(job_id, depends_on_id)` (also the "prerequisites of a job" index) |
| deleting a dependent removes its edges | `ON DELETE CASCADE` on `job_id` |
| a job others wait for cannot be deleted | foreign key on `depends_on_id`, `DEFERRABLE INITIALLY DEFERRED` (checked at COMMIT: a plain `NO ACTION` check can run before the cascade from a dependent has removed its edge, which would make deleting a whole workflow in one statement fail depending on row order) |
| edges only on a `PENDING` dependent | trigger `trg_job_dependencies_guard` (`TF004`) |
| edges are immutable | same trigger (`TF003` on update) |
| **no cycles** | not a constraint (PostgreSQL cannot express it); enforced by `DependencyService` under a global advisory lock, see below |

Indexes: `pk_job_dependencies (job_id, depends_on_id)` serves "prerequisites of job X" and the readiness check; `idx_job_dependencies_prerequisite (depends_on_id, job_id)` serves "dependents of job X" (failure propagation, descendants). A test asserts the planner uses each for its query.

**Readiness check.** In the trigger: `EXISTS (SELECT 1 FROM job_dependencies d JOIN jobs p ON p.id = d.depends_on_id WHERE d.job_id = NEW.id AND p.status <> 'COMPLETED')`, a lookup for one known job. In the scheduler queries, which test many candidate rows, the same rule is written as `NOT EXISTS (SELECT 1 FROM job_dependencies d WHERE d.job_id = jobs.id AND (SELECT p.status FROM jobs p WHERE p.id = d.depends_on_id) <> 'COMPLETED' OFFSET 0)`. The form is deliberate. `OFFSET 0` stops PostgreSQL from flattening the subquery into an anti-join (`LIMIT 1` does not work: PostgreSQL removes it from `EXISTS`), and fetching the status with a scalar primary-key lookup leaves no join method to choose badly. An earlier draft with a plain join was measured: when `job_dependencies` had no statistics (a fresh deployment) PostgreSQL could plan it as a hash join over the whole `jobs` table. Measured cost of the shipped form: about 3 to 5 microseconds per candidate row (`EXPLAIN ANALYZE`, 5,000 candidate rows: 6 ms without the filter, 19 to 32 ms with it, whether or not `job_dependencies` is analyzed and whether it holds 0 or 10,000 edges), i.e. it follows the number of candidates, not the table size. No counter column exists. A test asserts the plan (correlated sub-plan, primary-key lookups, no anti-join, no hash) both before and after `ANALYZE`.

**Trigger replaced.** Migration 0005 replaces `taskflow_guard_job_update()` (`CREATE OR REPLACE`; migration 0001 is untouched, as migrations are never edited after being applied): the transition table gains `('PENDING','FAILED')`, and a transition to `READY` is refused while any prerequisite is not `COMPLETED`. Everything else in the function is identical (immutable columns, version and `updated_at`).

**Upgrade and rollback.** 0005 adds one table and replaces one function; it changes no existing row, so a database that already applied 0001–0004 upgrades in place (tested with data present) and a fresh database reaches the same schema in one run. Migrations are forward-only; to undo 0005 write a new migration that drops `job_dependencies` (after removing its rows), drops its trigger function, and restores the previous `taskflow_guard_job_update()` body from 0001.

### Concurrency

*Cycle safety.* `AddDependencies` and `CreateWorkflow` run in one transaction that begins with `pg_advisory_xact_lock(727275)`. The lock is transaction-scoped, so it works across all application instances and is released at COMMIT/ROLLBACK, including after a crash (no stale lock). Everything that could create a cycle holds it while it (1) locks the involved jobs `FOR UPDATE` in ascending id order, (2) loads the stored ancestors of the new prerequisites (recursive CTE), (3) checks the union with the new edges for a cycle, and (4) inserts. Two opposite edges `X→Y` and `Y→X` are therefore strictly ordered: the second sees the first's committed edge and is refused with `CycleError`. This serializes graph *writes* only; reads, scheduling and job execution never take the lock. The cost is that dependency submission throughput is bounded by one transaction at a time (measured: `docs/benchmarks/dag-phase4.md`). Isolation level is READ COMMITTED; correctness comes from the lock, not from SERIALIZABLE.

*Failure racing with submission.* Adding an edge locks the prerequisite row, and a state change of the prerequisite takes the same row lock. If the prerequisite fails first, the new dependent is ended immediately in the submitting transaction; if the edge commits first, the failing transaction sees it and propagates. Either way a dependent is never stranded `PENDING`.

*Deadlocks and serialization failures.* Multi-row locks are taken in ascending id order, but a propagation (parent then child) and a submission can still lock in opposite orders. PostgreSQL then aborts one transaction with SQLSTATE `40P01` (or `40001`); every repository and service transaction retries up to five times with randomized, growing backoff (`RetryOnConflict`) before surfacing the error. The aborted attempt was fully rolled back.

*Release.* Promotion uses `FOR UPDATE SKIP LOCKED` and re-checks due time and prerequisites under the lock, so a job is released by exactly one scheduler; the trigger is the last line of defence.

## Scheduler queries (Phase 3)

No schema change was needed: the Phase 2 indexes were designed for these queries, and tests assert the planner uses them.

| Query (`JobRepository`) | Index | Notes |
|---|---|---|
| `FindDueJobs` — `status IN ('PENDING','RETRY_WAIT') AND priority = p AND scheduled_at <= now AND NOT EXISTS (unmet prerequisite) ORDER BY scheduled_at, sequence LIMIT n` (once per priority) | `idx_jobs_due` | oldest first; no payload columns are read; blocked jobs never fill the window |
| `PromoteJobsToReady` — `SELECT … WHERE id = ANY($1) AND status IN (…) AND scheduled_at <= now AND NOT EXISTS (unmet prerequisite) FOR UPDATE SKIP LOCKED`, `UPDATE … SET status = 'READY'`, `INSERT INTO job_events`, in one statement/transaction | primary key | `SKIP LOCKED` is what makes concurrent schedulers safe; the status/time predicate is re-checked under the lock |
| `FindReadyJobs` — `status = 'READY' AND priority = p AND (scheduled_at, sequence) > cursor ORDER BY scheduled_at, sequence LIMIT n` | `idx_jobs_ready_queue` | keyset pagination for reconciliation |
| `FindWaitingJobs` — `status IN ('PENDING','RETRY_WAIT') AND (scheduled_at, sequence) > cursor …` | `idx_jobs_due` | keyset pagination for reconciliation |
| `GetSchedulingEntries` — `WHERE id = ANY($1)` | primary key | verifies stale Redis entries before removal |

Promotion writes the same `PENDING|RETRY_WAIT → READY` transition the Phase 2 state machine already allows, and the same audit event (`reason = 'scheduled time reached'`).

## Conventions

- Timestamps are `timestamptz`; the C++ layer exchanges them as exact microseconds since the Unix epoch.
- `jobs.updated_at` uses `clock_timestamp()` (monotonic per row) so a long-running transaction can never make it earlier than `created_at`.
- Custom SQLSTATEs: `TF001` illegal state transition or initial state, `TF002` immutable column edit, `TF003` event modification. `JobRepository` maps these and the standard constraint errors (`23505`, `23514`, `23503`) onto domain errors (`InvalidTransitionError`, `ValidationError`, `ConflictError`, `NotFoundError`).
- A requeued (`FAILED`/`TIMEOUT` → `READY`) job keeps its `attempt_count`. If it has no attempts left, `CreateAttempt` fails with `ConflictError` until its retry policy is edited to allow more.

## Migrations

`NNNN_name.sql`, forward-only, each applied in its own transaction with its `schema_migrations` row. Applied files are checksummed; editing one is detected and refused. See `migrations/README.md`.
