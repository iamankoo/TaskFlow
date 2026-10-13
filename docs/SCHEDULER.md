# TaskFlow scheduler (Phase 3)

The scheduler answers one question: **which persisted job should become runnable next, by priority and scheduling time?** It moves jobs `PENDING`/`RETRY_WAIT` → `READY` and makes them available, in the right order, to the workers that arrive in Phase 5. It never executes jobs and never changes job state beyond that one transition.

Code: `include/taskflow/scheduler/` and `src/scheduler/`. Persistence queries: `JobRepository` (the "scheduler persistence" section). Tests: `tests/unit/priority_queue_test.cpp`, `tests/integration/scheduler*_test.cpp`, `tests/integration/schedule_queue_test.cpp`.

## Responsibilities

| Piece | Responsibility | Where |
|---|---|---|
| PostgreSQL | Authoritative job state; finds due jobs; performs the (locked, transactional) promotion | `JobRepository::FindDueJobs`, `PromoteJobsToReady` |
| `JobPriorityQueue` | The scheduling **decision**: an in-memory indexed binary heap ordering candidates | `priority_queue.hpp` |
| `PriorityPolicy` | The one definition of the order: priority, FIFO, aging | `priority_queue.hpp` |
| `ScheduleQueue` | The Redis structures workers will pull from | `schedule_queue.hpp` |
| `Scheduler` | The tick loop, publishing, reconciliation, lifecycle | `scheduler.hpp` |

The HTTP layer and the repository contain no scheduling logic. The repository only retrieves and persists; every ordering decision is made by `PriorityPolicy` through the heap.

## One tick

```
1. FindDueJobs(now)            PostgreSQL, idx_jobs_due: PENDING/RETRY_WAIT with scheduled_at <= now and no unmet
                               prerequisite, up to candidates_per_priority per priority, oldest first
2. heap = JobPriorityQueue     push every candidate; pop the best max_promotions_per_tick
3. PromoteJobsToReady(ids)     ONE transaction: lock rows (FOR UPDATE SKIP LOCKED), re-check due + prerequisites,
                               PENDING/RETRY_WAIT -> READY,
                               one job_events row each; rows another scheduler holds are skipped
4. MarkReady(promoted)         Redis, one Lua script: ZREM from delayed + ZADD to the priority's ready queue
5. Reconcile (if due)          every reconcile_interval_ticks, on a missing sentinel, or after a Redis error
```

PostgreSQL commits at step 3 before Redis is touched. If Redis fails, the tick still succeeded; the error is recorded and the next tick reconciles. Between ticks the loop waits `tick_interval_ms` on a condition variable (never a busy loop) and `Stop()` interrupts the wait, so shutdown is prompt.

## Priority, FIFO and aging

Levels, most urgent first: `CRITICAL`, `HIGH`, `NORMAL`, `LOW`. The numeric mapping exists in exactly one function (`PriorityPolicy::Level`); everything else iterates `kPrioritiesHighestFirst` or uses the enum.

Order (`PriorityPolicy::Before`, a strict total order):

1. higher priority first;
2. equal priority: earlier `scheduled_at` first, then lower `sequence` (submission order, from Phase 2) — **FIFO**;
3. with aging on, "priority" is the effective level below.

**Aging.** Every `aging_interval` a job has waited (measured from `scheduled_at`) counts as one extra priority level, so a `LOW` job overtakes a fresh `CRITICAL` job after three intervals and cannot be starved. The persisted priority is never changed; aging only influences the order. It is deterministic: job A outranks job B when `level_a + (now - t_a)/I > level_b + (now - t_b)/I`; `now` cancels, leaving the static key `t - level × I` (smaller first, integer microseconds). Because the key does not depend on the clock, a plain heap stays correct as time passes and no re-sorting is needed. `aging_interval_ms = 0` disables aging (strict priority).

This is the continuous form of the architecture's "promoted one level after a threshold": same bounded wait, but a smooth rule that a heap and a Redis script can both evaluate exactly.

## The priority queue

An indexed binary min-heap (`std::vector` + `unordered_map<id, index>`), ordered by `PriorityPolicy`.

| Operation | Cost |
|---|---|
| `Push` (new or replace) | O(log n) |
| `Pop` (highest priority) | O(log n) |
| `Remove(id)` (invalidate a stale entry) | O(log n) |
| `Top`, `Contains`, `size` | O(1) |
| `RemoveIf(predicate)` | O(n) |

Duplicate protection: one entry per job id; pushing an id again replaces its entry and re-sorts. Not thread-safe by design (each tick builds its own).

## Redis structures

All keys start with `scheduler.redis_prefix` (default `taskflow`). Redis stores **identifiers and sort keys only**, never payloads or job state.

| Key | Type | Content |
|---|---|---|
| `{prefix}:ready:CRITICAL`, `:HIGH`, `:NORMAL`, `:LOW` | sorted set | READY jobs of that priority |
| `{prefix}:delayed` | sorted set | PENDING and RETRY_WAIT jobs (the delayed index) |
| `{prefix}:lock:reconcile` | string, TTL | short-lived reconciliation lock |
| `{prefix}:sentinel` | string | present once Redis has been reconciled |

Member: `"<sequence, 20 digits zero-padded>:<job uuid>"`. Score: `scheduled_at` in microseconds since the epoch (exact in a double until the year 2255).

Why sorted sets and not lists: a list is FIFO by *push* time, but delayed and retried jobs must come out by `scheduled_at`; a sorted set gives that order, O(log n) insert/remove, atomic removal of an arbitrary member and cheap range reads for reconciliation. Why zero-padded sequence in the member: members with equal scores sort lexicographically, so padded sequences make ties FIFO with no extra data. Why one set per priority instead of one combined set: `LLEN`/`ZCARD` per priority is a free queue-depth metric, and an operator can inspect or drain one level.

**Dequeue** (`ScheduleQueue::PopNext`, one Lua script, atomic): look at the head of each of the four queues, pick the best by exactly the `PriorityPolicy` rule (including aging), `ZREM` it, return it. A test compares this script with the C++ heap on random data. Popping from Redis does not claim the job: in Phase 5 a worker must still win the PostgreSQL `READY → RUNNING` transition (`CreateAttempt`). A job popped by a worker that then dies is still READY in PostgreSQL, so reconciliation puts it back; two workers racing for it are separated by PostgreSQL, so stale or duplicated queue entries are harmless.

Delayed jobs: `PENDING`/`RETRY_WAIT` jobs live in the delayed set (kept by reconciliation and by `Scheduler::Publish`). Due detection itself uses PostgreSQL's `idx_jobs_due`, so a flushed Redis never delays a promotion; the delayed set is the fast index for consumers and for inspection.

## Reconciliation: PostgreSQL wins

`Scheduler::Reconcile()` repairs Redis so that it matches PostgreSQL:

1. take `{prefix}:lock:reconcile` (`SET NX PX lock_ttl_ms`, owner token; released by compare-and-delete Lua). If another scheduler holds it, skip — and if that scheduler crashed, the TTL expires the lock;
2. read what PostgreSQL says: READY jobs per priority and PENDING/RETRY_WAIT jobs, paged by keyset `(scheduled_at, sequence)` (`scan_batch_size` rows per page, using `idx_jobs_ready_queue` / `idx_jobs_due`);
3. read what Redis holds (paged `ZRANGE … WITHSCORES`);
4. **add** what is missing or wrongly scored; **remove** what Redis holds but PostgreSQL does not expect — after re-checking each candidate against PostgreSQL (a job another scheduler promoted a moment ago is not stale);
5. set the sentinel.

Repaired situations (all tested): READY in PostgreSQL but absent from Redis; in Redis but COMPLETED/CANCELLED/unknown in PostgreSQL; in the wrong priority queue; in the delayed set but READY; wrong score; malformed members; total loss (`FLUSHDB`). Reconciliation only writes to Redis; it never modifies job state.

Redis loss is noticed within one tick: a missing sentinel triggers an immediate reconcile.

## Several schedulers

Any number of scheduler instances can run on the same database:

- **Promotion** is protected by PostgreSQL: `FOR UPDATE SKIP LOCKED` plus a status predicate re-checked under the lock. A job is promoted by exactly one scheduler; the others skip it. No global lock.
- **Redis writes** (`ZADD`, `ZREM`, the `MarkReady` script) are idempotent and commute.
- **Reconciliation** takes a short-lived, owner-checked lock with a TTL, so only one instance repairs at a time and a crashed one cannot block the rest.

Test: 5 schedulers in 5 threads work the same 100 due jobs with small batches: 100 promotions, 100 `READY` events (one per job), every row at version 2, zero other states, and Redis converges to exactly the 100.

## Dependencies (Phase 4)

The scheduler needed one rule for DAGs: **a job with an unmet prerequisite is not a candidate and cannot be promoted.** Both `FindDueJobs` and `PromoteJobsToReady` add `AND NOT EXISTS (… a prerequisite whose status <> 'COMPLETED')`; the promotion re-checks it under the row lock, and the database trigger refuses `READY` otherwise. Consequences:

- The priority queue, aging, Redis structures and reconciliation are unchanged. Blocked jobs are simply `PENDING`, so reconciliation lists them in the delayed set and never in a ready queue; a forged ready entry for a blocked job is removed because PostgreSQL says `PENDING`.
- Blocked jobs cannot crowd out runnable ones: they are filtered in SQL, before the `candidates_per_priority` window.
- A job released by its last prerequisite is promoted on the next tick (latency ≤ `tick_interval_ms`), by exactly one scheduler.
- Failure propagation does not involve the scheduler: it happens in the transaction that ends the prerequisite. The scheduler only runs the periodic safety net (`DependencyService::ResolveBlocked`) for jobs stranded by changes made outside the repository.

## Configuration

`scheduler.*` (JSON) or `TASKFLOW_SCHEDULER_*` (environment); see `.env.example`: `enabled`, `tick_interval_ms` (1000), `max_promotions_per_tick` (500), `candidates_per_priority` (500), `aging_interval_ms` (60000; 0 = off), `reconcile_interval_ticks` (10), `scan_batch_size` (500), `lock_ttl_ms` (10000), `redis_prefix` (`taskflow`). The application starts the scheduler after the HTTP listener setup and stops it (finishing the current tick) when the server stops.

## Known limits

- Reconciliation holds the full expected and actual sets in memory (a few hundred bytes per job); fine to hundreds of thousands of jobs, to be streamed in Phase 9 if measurements demand it.
- Candidate selection reads at most `candidates_per_priority` due jobs per priority per tick, oldest first. A backlog larger than that is drained over several ticks; within the window the heap order is exact.
- Leases (Phase 6) and retry policy (Phase 7) are not part of the scheduler yet.
