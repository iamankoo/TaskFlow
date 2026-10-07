# Migrations

Versioned, forward-only SQL migrations applied by TaskFlow at startup (`app.auto_migrate`) or on demand with `taskflow --migrate-only`.

## Rules

- File name: `NNNN_lowercase_name.sql`, e.g. `0001_create_jobs.sql`. Versions are unique and increasing.
- Each file runs in its own transaction together with its bookkeeping row in `schema_migrations`. Do **not** put `BEGIN`/`COMMIT` in a migration.
- Never edit a migration after it has been applied anywhere: the runner stores a checksum and refuses to start if an applied file changed. Add a new migration instead.
- A migration older than the latest applied one is rejected (no out-of-order history).
- Concurrent runners are safe (PostgreSQL advisory lock).

## Status

Phase 1 ships the migration **mechanism** only (the runner, the `schema_migrations` tracking table and its tests). There are intentionally no application tables yet: the job schema is Phase 2.
