# TaskFlow

A distributed job execution platform in C++20: REST API, priority scheduler, delayed jobs, DAG dependencies, concurrent worker pool, lease/heartbeat failure recovery, retries and idempotency. PostgreSQL is the durable source of truth; Redis is a fast, rebuildable coordination layer.

> **Status:** Phase 4 of 10 complete (foundation, persistent job model, scheduler, DAG dependencies). Jobs are stored in PostgreSQL, can wait for other jobs (cycle-checked dependency graphs), and the scheduler promotes due, unblocked jobs to READY in priority order and queues them in Redis, but nothing executes them yet and there is no job API. See [`Phases.md`](Phases.md) for the roadmap, [`ARCHITECTURE.md`](ARCHITECTURE.md) for the design, [`TECHSTACK.md`](TECHSTACK.md) for the locked stack and [`summary.md`](summary.md) for what is actually implemented.

## Quick start (Docker)

Requires Docker with Compose v2. No local C++ toolchain is needed.

```bash
docker compose up --build            # TaskFlow + PostgreSQL + Redis
curl http://127.0.0.1:8080/health    # {"status":"ok", ...}
```

Run the whole test suite (unit, integration and HTTP) against the Compose services:

```bash
docker compose --profile test run --rm taskflow-test
```

Stop and remove everything including the database volume: `docker compose down -v`.

## Building natively (Linux)

Prerequisites: GCC 13+ or Clang 17+, CMake 3.25+, Ninja, and the packages `autoconf automake bison flex libtool pkg-config uuid-dev zip unzip curl git`. Dependencies are managed with [vcpkg](https://github.com/microsoft/vcpkg) in manifest mode, pinned to the commit in `vcpkg.json` (`builtin-baseline`).

```bash
git init ~/vcpkg && git -C ~/vcpkg remote add origin https://github.com/microsoft/vcpkg.git
git -C ~/vcpkg fetch --depth 1 origin 4b308f5ccd45aed57f2977257bca304dfef43c99
git -C ~/vcpkg checkout FETCH_HEAD && ~/vcpkg/bootstrap-vcpkg.sh -disableMetrics
export VCPKG_ROOT=~/vcpkg

cmake --preset release            # first run builds all dependencies (slow, then cached)
cmake --build --preset release
```

Presets: `debug`, `release`, `asan` (AddressSanitizer + UBSan), `tsan` (ThreadSanitizer), `ci` (what CI runs).

## Tests

| Suite | Needs services | Label | What it covers |
|---|---|---|---|
| `taskflow_unit_tests` | no | `unit` | config, errors, resource pool, logging, health service, migration loading, job state machine, job model validation, priority queue and aging, dependency graph (cycles, topological order, large graphs) |
| `taskflow_integration_tests` | PostgreSQL, Redis | `integration` | connectivity, pooling, migration runner, Redis commands, application failure paths, job schema constraints and triggers, job repository, transaction rollback, scheduler, Redis queues, reconciliation, multi-scheduler safety, dependency schema and upgrade, cycle safety under concurrency, failure propagation, DAG scheduling |
| `taskflow_http_tests` | PostgreSQL, Redis | `http` | the real server and `GET /health` over TCP, graceful shutdown |

```bash
ctest --preset unit        # no services required
ctest --preset ci          # everything; needs PostgreSQL and Redis (see .env.example for TASKFLOW_* variables)
```

Integration tests fail, rather than skip, when a service is unreachable.

## Configuration

Precedence: built-in defaults < JSON file (`--config FILE` or `TASKFLOW_CONFIG_FILE`) < `TASKFLOW_*` environment variables. Unknown JSON keys are rejected. [`.env.example`](.env.example) lists every variable. Copy it to `.env` for local use; `.env` is gitignored.

```json
{ "http": { "port": 8080 }, "postgres": { "host": "localhost", "pool_size": 8 }, "logging": { "format": "json" } }
```

## Command line

```
taskflow [--config FILE] [--migrate-only] [--version] [--help]
```

Exit codes: `0` ok, `1` runtime failure, `2` bad arguments, `3` invalid configuration.

## Layout

```
include/taskflow/   public headers, one directory per module
src/                implementations (common, config, logging, db, redis, api, app)
tests/              unit/, integration/, http/, support/
migrations/         versioned SQL (job schema: see docs/DATABASE.md)
bench/              scheduler and DAG baseline benchmarks (`taskflow_scheduler_bench`, `taskflow_dag_bench`)
docker/             Dockerfile (toolchain, deps, build, test, runtime stages)
.github/workflows/  CI
```

| Module | Responsibility |
|---|---|
| `common` | error types, generic bounded resource pool |
| `jobs` | job domain model and the job state machine |
| `scheduler` | priority heap and aging policy, Redis queues, the scheduler tick and reconciliation (`docs/SCHEDULER.md`) |
| `dag` | in-memory dependency graph (cycle detection, topological order) and `DependencyService` (cycle-safe edge and workflow creation, propagation, inspection) |
| `config` | defaults, JSON file, environment overrides, validation |
| `logging` | spdlog setup, text or JSON output |
| `db` | libpq connection, pool and transactions, forward-only migration runner, `JobRepository` (all job SQL) |
| `redis` | hiredis connection and pool |
| `api` | Drogon HTTP server, `/health` |
| `app` | startup, dependency wait, shutdown, signal handling |

## `GET /health`

Returns `200` with `{"status":"ok","version":...,"uptime_seconds":...,"dependencies":{"postgres":{...},"redis":{...}}}` when the process is up and both dependencies answer. Returns `503` with `status` `degraded` (a dependency is down) or `shutting_down` (graceful shutdown in progress). Error details are logged server-side and never returned to clients.
