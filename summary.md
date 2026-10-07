# TaskFlow — Living Project Summary

> Updated after **every** completed phase. Only implemented **and validated** work is recorded as done. Authority order: `Phases.md` → `ARCHITECTURE.md` → `TECHSTACK.md` → `summary.md`.

## Project Overview

TaskFlow is a distributed job execution platform in C++20: REST API, priority scheduler, delayed jobs, DAG dependencies, concurrent worker pool, leasing/heartbeat-based failure recovery, retries, timeouts, and idempotency. PostgreSQL is the durable source of truth; Redis provides fast coordination. Repository: https://github.com/iamankoo/TaskFlow

## Current Status

Documentation initialized and locked. **No implementation code exists yet.**

## Current Phase

None started. Awaiting owner's go-ahead for Phase 1.

## Completed Phases

None.

## Phase-by-Phase Summary

| # | Phase | Status | Commit |
|---|-------|--------|--------|
| 1 | Foundation & Project Infrastructure | Not started | — |
| 2 | Job Model & Persistent State | Not started | — |
| 3 | Scheduler & Priority Queue | Not started | — |
| 4 | DAG Dependency Engine | Not started | — |
| 5 | Concurrent Worker Pool | Not started | — |
| 6 | Job Leasing, Heartbeats & Failure Recovery | Not started | — |
| 7 | Retries, Timeouts & Idempotency | Not started | — |
| 8 | REST API & Production Operations | Not started | — |
| 9 | Testing, Load Testing & Performance Engineering | Not started | — |
| 10 | Final Hardening, Benchmark & Release (FINAL) | Not started | — |

## Implemented Features

None yet.

## Architecture Summary

Planned (see `ARCHITECTURE.md`): Clients → REST API → TaskFlow Core (Scheduler, Priority Queue, Delayed Jobs, DAG Engine, Retry Manager, Lease Manager, Worker Manager) → Worker Pool → PostgreSQL + Redis. PostgreSQL is authoritative; Redis is rebuildable from PostgreSQL. Delivery is at-least-once with fencing tokens.

## Technology Stack

Locked (see `TECHSTACK.md`): C++20, CMake, Drogon, PostgreSQL, Redis, Docker, Docker Compose, GoogleTest, spdlog, JSON, Prometheus-compatible metrics, GitHub Actions.

## Database Summary

Not implemented. Planned tables: `jobs`, `job_dependencies`, `job_attempts`, `workers`, `job_events` (Phase 2).

## API Summary

Not implemented. Planned base path `/api/v1` plus `/healthz`, `/readyz`, `/metrics` (Phase 8; `/healthz` stub in Phase 1).

## Testing Summary

No tests yet.

## Performance Benchmarks

None measured. No performance claims are made until measured (Phases 9–10).

## Known Issues

None (no code yet).

## Important Decisions

| Date | Decision |
|------|----------|
| 2026-10-07 | Locked 10-phase roadmap, architecture, and tech stack. |
| 2026-10-07 | PostgreSQL = durable source of truth; Redis = rebuildable coordination layer. |
| 2026-10-07 | Single binary with role flags (`api`, `scheduler`, `worker`, `all`). |
| 2026-10-07 | At-least-once delivery with fencing tokens; exactly-once is not claimed. |
| 2026-10-07 | All commits/pushes use `iamankoo <aniketraj00384@gmail.com>` only; no AI attribution. |

## Git Commit History

| Commit | Message | Phase |
|--------|---------|-------|
| _see below_ | docs: initialize TaskFlow architecture and development roadmap | Initialization |

_The initialization commit hash is recorded in the next documentation update, since a commit cannot contain its own hash._

## Next Phase

**Phase 1 — Foundation & Project Infrastructure** (not started; begins only on the owner's instruction).
