# TaskFlow — Living Project Summary

> Updated after **every** completed phase. Only implemented **and validated** work is recorded as done. Authority order: `Phases.md` → `ARCHITECTURE.md` → `TECHSTACK.md` → `summary.md`.

## Project Overview

TaskFlow is a distributed job execution platform in C++20: REST API, priority scheduler, delayed jobs, DAG dependencies, concurrent worker pool, leasing/heartbeat-based failure recovery, retries, timeouts, and idempotency. PostgreSQL is the durable source of truth; Redis provides fast coordination. Repository: https://github.com/iamankoo/TaskFlow

## Current Status

**Phase 1 complete.** The repository builds from a clean tree, all 75 tests pass (also under ASan + UBSan), and the Docker Compose stack (TaskFlow + PostgreSQL + Redis) starts healthy and serves `GET /health`. No job functionality exists yet: that starts in Phase 2+.

GitHub Actions CI is configured and uses the same commands as the local validation, but **its first run on GitHub had not been observed when this was written** (see Known Issues).

## Current Phase

None in progress. Phase 1 is finished; Phase 2 starts only on the owner's instruction.

## Completed Phases

- **Phase 1 — Foundation & Project Infrastructure.** Commit hash: see "Git Commit History" (a commit cannot contain its own hash; it is recorded in the first commit of Phase 2).

## Phase-by-Phase Summary

| # | Phase | Status | Commit |
|---|-------|--------|--------|
| 1 | Foundation & Project Infrastructure | **Complete** | see Git Commit History |
| 2 | Job Model & Persistent State | Not started | — |
| 3 | Scheduler & Priority Queue | Not started | — |
| 4 | DAG Dependency Engine | Not started | — |
| 5 | Concurrent Worker Pool | Not started | — |
| 6 | Job Leasing, Heartbeats & Failure Recovery | Not started | — |
| 7 | Retries, Timeouts & Idempotency | Not started | — |
| 8 | REST API & Production Operations | Not started | — |
| 9 | Testing, Load Testing & Performance Engineering | Not started | — |
| 10 | Final Hardening, Benchmark & Release (FINAL) | Not started | — |

### Phase 1 detail

**Built and validated**

- C++20 CMake project: static library `taskflow_core`, executable `taskflow`, three test executables. Warnings (`-Wall -Wextra -Wpedantic -Wshadow`) are errors for project code; sanitizer presets (`asan` = ASan+UBSan, `tsan`).
- **Configuration** (`config`): defaults < optional JSON file < `TASKFLOW_*` environment. Strict (unknown keys, wrong types, out-of-range values are errors). Startup log line never contains secrets.
- **Logging** (`logging`): spdlog, text or JSON (one escaped JSON object per line, UTC timestamps). Drogon's own log output is forwarded into the same stream.
- **Errors** (`common`): `Error` hierarchy with stable machine-readable codes (`config_error`, `database_error`, `redis_error`, `migration_error`, `pool_timeout`, `lifecycle_error`); generic thread-safe bounded `ResourcePool`.
- **PostgreSQL** (`db`): libpq wrapper with parameterized queries and a pooled, validated connection pool; forward-only **migration runner** (versioned `NNNN_name.sql`, per-migration transaction, checksum of applied files, out-of-order and removed-file detection, advisory lock for concurrent runners).
- **Redis** (`redis`): hiredis wrapper (AUTH, SELECT, timeouts, binary-safe arguments, error replies as exceptions) and pool. **Connection layer only; no queues.**
- **HTTP** (`api`): Drogon server with `GET /health`; health probes run off the event-loop threads.
- **Lifecycle** (`app`): waits for PostgreSQL and Redis (bounded retry), runs migrations, serves; SIGINT/SIGTERM handled on a dedicated `sigwait` thread (not in signal-handler context); graceful shutdown reports `shutting_down` for a configurable grace period, then exits `0`. CLI: `--config`, `--migrate-only`, `--version`, `--help`; exit codes 0/1/2/3.
- **Docker**: multi-stage `docker/Dockerfile` (toolchain, deps, build, test, runtime); runtime image runs as non-root (uid 10001), 155 MB. `docker-compose.yml` runs `postgres:16-alpine`, `redis:7-alpine`, `taskflow`, plus a `taskflow-test` service (profile `test`).
- **CI**: `.github/workflows/ci.yml` — Release (`ci`) and ASan+UBSan builds with PostgreSQL/Redis service containers, vcpkg pin consistency check, and a Docker image + Compose `/health` job.
- Repo hygiene: `.gitignore` (env files and secrets excluded), `.gitattributes` (LF), `.dockerignore`, `.clang-format`, `.clang-tidy` (advisory, never run in CI), `.env.example`, `README.md`.

**Not built (by design, belongs to later phases):** job schema or tables (the only table is `schema_migrations`), scheduling, queues, DAGs, workers, leasing, heartbeats, retries, timeouts, idempotency, any job API.

## Implemented Features

- Build, dependency, configuration, logging, error-handling, PostgreSQL, Redis, migration, HTTP-server, lifecycle, Docker and CI foundations as listed above.
- `GET /health` only.

## Architecture Summary

As in `ARCHITECTURE.md`. Phase 1 implements the infrastructure beneath it: single binary, PostgreSQL pool, Redis pool, HTTP server, lifecycle. Components such as Scheduler, Lease Manager and Worker Manager do not exist yet.

## Technology Stack

Locked (see `TECHSTACK.md`): C++20, CMake, Drogon, PostgreSQL, Redis, Docker, Docker Compose, GoogleTest, spdlog, JSON, Prometheus-compatible metrics, GitHub Actions.

Exact versions used in Phase 1 (vcpkg baseline `4b308f5ccd45aed57f2977257bca304dfef43c99`): Drogon 1.9.13, spdlog 1.17.0, libpq 18.4 (client library), hiredis 1.4.1, jsoncpp 1.9.6, GoogleTest 1.18.0, GCC/CMake from Ubuntu 24.04. Runtime services in Compose: PostgreSQL 16, Redis 7.

**Package manager: vcpkg, manifest mode** (`vcpkg.json`, baseline pinned). The pin is duplicated in `docker/Dockerfile` and `ci.yml`; CI fails if the three disagree.

## Database Summary

- Only `schema_migrations (version BIGINT PK, name, checksum, applied_at)`, created by the runner itself. No migration files ship in Phase 1 (`migrations/README.md` documents the rules).
- The job schema (`jobs`, `job_dependencies`, `job_attempts`, `workers`, `job_events`) is Phase 2.

## API Summary

| Endpoint | Behavior |
|---|---|
| `GET /health` | `200` `{"status":"ok","version","uptime_seconds","dependencies":{"postgres":{"status","latency_ms"},"redis":{...}}}`. `503` with `status` `degraded` (dependency down, `error` = error code only) or `shutting_down`. `Cache-Control: no-store`. Other methods → `405`, unknown paths → `404`. |

## Testing Summary

75 tests, all passing, run in Docker against real PostgreSQL 16 and Redis 7:

| Suite | Tests | Covers |
|---|---|---|
| `taskflow_unit_tests` (`unit`) | 39 | config (defaults, JSON, env precedence, strictness, validation, secret redaction), error types, resource pool (reuse, timeout, broken/stale resources, contention), logging (JSON escaping, validity), health service (ok/degraded/shutting_down, no error-text leakage), migration file loading and checksums |
| `taskflow_integration_tests` (`integration`) | 29 | PostgreSQL connectivity, params/NULL/injection safety, SQL errors, unreachable server, bad password, pool under 12 threads, recovery after server-side disconnect; migration runner (ordering, idempotency, edit/removal/out-of-order detection, rollback of a failing migration, 4 concurrent runners); Redis (string/binary/integer/array replies, error replies, unreachable, AUTH failure, concurrent pool); application failure paths (uninitialized use, unreachable PostgreSQL/Redis, missing migrations dir, shutdown during startup) |
| `taskflow_http_tests` (`http`) | 7 | real server over TCP: `/health` ok with both dependencies up, JSON/no-store headers, migration bookkeeping ran at startup, 404, 405, 20 concurrent requests, graceful shutdown (`503 shutting_down` then clean exit 0) |

Results:

- Release build with `-Werror` (the `ci` preset): **75/75 pass**, 0 skipped.
- ASan + UBSan build: **75/75 pass**, 0 skipped, no sanitizer reports.
- The HTTP suite was additionally repeated 8× under ASan: all pass.
- Not run: ThreadSanitizer (preset exists; first needed in Phase 5), clang-tidy, any native Windows build.

Manual validation of the live Compose stack:

- All three services reach `healthy`; `taskflow` reaches both dependencies over the Compose network; `/health` returns `200` with both up.
- Stopping Redis → `503 degraded` naming `redis`; restarting → `200`. Same for PostgreSQL.
- `docker compose stop taskflow` → logs `received signal 15`, drains 1 s, exit code 0, total 2.4 s.
- `taskflow --migrate-only` applies and exits 0; bad configuration exits 3; unreachable Redis after the startup timeout exits 1; `--version`, JSON logs verified.

## Performance Benchmarks

None measured. No performance claims are made until measured (Phases 9–10).

## Known Issues

1. **CI not yet observed.** The workflow mirrors the validated local commands but has never run on GitHub (first run is uncached and may take 20–40 min). Treat CI as unverified until a green run is seen. Differences from local: it uses the runner's GCC rather than the Docker image's, and actions versions are unpinned by digest.
2. **`/health` latency is not bounded.** The Redis/PostgreSQL connect timeouts do not cover DNS resolution. When a Compose service is stopped and its hostname disappears, `/health` took ~12 s to answer `503`. A per-probe deadline is worth adding (Phase 8, with production operations).
3. **One unreproduced test-startup flake.** During one ASan run, a single HTTP test was skipped because the server had not accepted connections within 10 s; 12 isolated reruns and 8 full repeats passed. The fixture now fails the test loudly instead of skipping, and the server-readiness wait now checks that the port accepts connections (a genuine race fixed along the way). If the flake returns it will appear as a failure with a message.
4. `ctest` counts a skipped test as passing. Phase 1 has exactly one conditional skip (`PostgresTest.WrongPasswordIsRejected` when the server uses trust authentication); it did not skip in any validated run.
5. Linux only (POSIX signals, sockets). Windows development is through Docker.
6. `.clang-tidy` is advisory and unrun; checksum for migrations is FNV-1a (accidental-edit detection, not tamper-proofing).
7. Local port 5432/6379 were occupied by an unrelated project on the dev machine, so the local `.env` (untracked) publishes Compose services on 15432/16379. `.env.example` keeps the defaults.

## Important Decisions

| Date | Decision |
|------|----------|
| 2026-10-07 | Locked 10-phase roadmap, architecture, and tech stack. |
| 2026-10-07 | PostgreSQL = durable source of truth; Redis = rebuildable coordination layer. |
| 2026-10-07 | Single binary with role flags (`api`, `scheduler`, `worker`, `all`); Phase 1 implements the process skeleton only, roles arrive with their phases. |
| 2026-10-07 | At-least-once delivery with fencing tokens; exactly-once is not claimed. |
| 2026-10-07 | All commits/pushes use `iamankoo <aniketraj00384@gmail.com>` only; no AI attribution. |
| 2026-10-07 | **vcpkg (manifest mode, pinned baseline)** is the dependency manager. |
| 2026-10-07 | **libpq and hiredis used directly** through small pooled wrappers instead of Drogon's async DB/Redis clients, which are tied to Drogon's event loops while the scheduler and workers need blocking, thread-safe access. Drogon is HTTP only. `TECHSTACK.md` updated. |
| 2026-10-07 | **Single `/health` endpoint** (liveness + dependency connectivity + shutdown state) replaces the planned `/healthz` and `/readyz`, per the Phase 1 instruction. `Phases.md` and `ARCHITECTURE.md` updated. `/metrics` remains for Phase 8. |
| 2026-10-07 | Signals are handled on a `sigwait` thread, not in Drogon's signal handlers, so shutdown code may safely lock, allocate and log. |
| 2026-10-07 | Phase 1 ships the migration mechanism but **no application tables**; the job schema is Phase 2. |
| 2026-10-07 | Integration tests fail (not skip) when PostgreSQL/Redis are unreachable. |

## Git Commit History

| Commit | Message | Phase |
|--------|---------|-------|
| `585b4b5` | docs: initialize TaskFlow architecture and development roadmap | Initialization |
| _next commit on `main`_ | feat: complete TaskFlow phase 1 foundation | Phase 1 |

The Phase 1 commit hash is reported in the Phase 1 completion report and written into this table by the first commit of Phase 2.

## Next Phase

**Phase 2 — Job Model & Persistent State** (not started; begins only on the owner's instruction).
