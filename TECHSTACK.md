# TaskFlow — Technology Stack (Locked)

Governed by `Phases.md` and `ARCHITECTURE.md`. This list is **locked**. No framework or technology may be added, replaced, or removed without a documented rationale, owner approval, and a corresponding update to `ARCHITECTURE.md` and the "Important Decisions" section of `summary.md`.

| Technology | Role | Why |
|------------|------|-----|
| **C++20** | Implementation language | Predictable performance and memory control for a throughput-sensitive scheduler/worker; modern features (concepts, `std::span`, `std::jthread`, `std::chrono` improvements, designated initializers) improve safety and clarity. |
| **CMake** | Build system | De facto standard for C++; cross-platform; integrates with CTest, GoogleTest, sanitizers, and CI. |
| **Drogon** | REST/HTTP framework | High-performance asynchronous C++ web framework with event-loop model, built-in JSON and routing, and PostgreSQL/Redis clients, suited to a low-latency API. |
| **PostgreSQL** | Durable source of truth | ACID transactions, row-level locking with `SKIP LOCKED`, constraints, and indexes provide the consistency the job state machine, leases, and DAG release require. |
| **Redis** | Fast coordination | Sub-millisecond queues (lists/sorted sets), atomic Lua scripts, and TTL keys fit ready queues, delayed jobs, heartbeats, and short-lived locks. Never authoritative. |
| **Docker** | Packaging | Reproducible builds and runtime; minimal non-root runtime images. |
| **Docker Compose** | Local/dev/prod-like orchestration | Defines the multi-service topology (API, scheduler, workers, PostgreSQL, Redis) and enables failure-injection testing. |
| **GoogleTest** | Testing | Mature C++ unit/integration test framework with fixtures, parameterized tests, and CTest integration. |
| **spdlog** | Logging | Fast, header-friendly, structured and leveled logging with async sinks. |
| **JSON** | Data interchange | Request/response bodies and job payloads. Implementation uses the JSON facilities provided by Drogon (JsonCpp) at API boundaries; `nlohmann/json` may be used internally only if recorded in `summary.md` as a decision. |
| **Prometheus-compatible metrics** | Observability | Text exposition format on `/metrics`, scrapeable by Prometheus and compatible systems; no Prometheus server is required by TaskFlow itself. |
| **GitHub Actions** | CI | Automated build, test, and sanitizer runs on every push/PR. |

## Supporting Tooling (non-architectural)

Development aids that do not alter the architecture: `clang-format`, `clang-tidy`, AddressSanitizer / UndefinedBehaviorSanitizer / ThreadSanitizer, and a C++ dependency manager (vcpkg or Conan). The dependency manager is **vcpkg in manifest mode** (decided in Phase 1; `vcpkg.json` pins the baseline commit). Load-testing and benchmark tooling in Phase 9 must be recorded in `summary.md` and kept out of the production image.

## Constraints

- Language standard: C++20 only (no compiler-specific extensions in project code).
- Compilers targeted: GCC and Clang on Linux (primary, CI and Docker); local Windows development is via Docker/WSL.
- Database access: **`libpq`** and **`hiredis`** used directly through small pooled wrappers (decided in Phase 1). Drogon's async DB/Redis clients are bound to its event loops, while the scheduler and workers need blocking, thread-safe access. Drogon is used for HTTP only.
- No additional message brokers, ORMs, or service frameworks.
