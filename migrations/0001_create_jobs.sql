-- Phase 2: the durable job record. PostgreSQL is the source of truth for every job.
-- See docs/DATABASE.md for the design notes behind each decision.

CREATE TABLE jobs (
    id                        uuid             NOT NULL DEFAULT gen_random_uuid(),
    -- Global submission order; the FIFO tie-breaker for the Phase 3 scheduler.
    sequence                  bigint           GENERATED ALWAYS AS IDENTITY,
    type                      text             NOT NULL,
    payload                   jsonb            NOT NULL DEFAULT '{}'::jsonb,
    metadata                  jsonb            NOT NULL DEFAULT '{}'::jsonb,
    -- 0 LOW, 1 NORMAL, 2 HIGH, 3 CRITICAL: a descending sort is scheduling order.
    priority                  smallint         NOT NULL DEFAULT 1,
    status                    text             NOT NULL DEFAULT 'PENDING',
    -- Earliest time the job may run (delayed jobs and retry backoff both use it).
    scheduled_at              timestamptz      NOT NULL DEFAULT now(),
    max_attempts              integer          NOT NULL DEFAULT 3,
    attempt_count             integer          NOT NULL DEFAULT 0,
    retry_initial_delay_ms    integer          NOT NULL DEFAULT 1000,
    retry_backoff_multiplier  double precision NOT NULL DEFAULT 2.0,
    retry_max_delay_ms        integer          NOT NULL DEFAULT 300000,
    timeout_ms                integer          NOT NULL DEFAULT 300000,
    idempotency_key           text,
    last_error                text,
    -- Optimistic concurrency token; incremented by trigger on every update.
    version                   bigint           NOT NULL DEFAULT 1,
    created_at                timestamptz      NOT NULL DEFAULT now(),
    updated_at                timestamptz      NOT NULL DEFAULT now(),
    finished_at               timestamptz,

    CONSTRAINT pk_jobs PRIMARY KEY (id),
    CONSTRAINT uq_jobs_sequence UNIQUE (sequence),
    -- Phase 7 builds idempotent submission on this; NULLs never collide.
    CONSTRAINT uq_jobs_idempotency_key UNIQUE (idempotency_key),

    CONSTRAINT ck_jobs_type CHECK (char_length(type) BETWEEN 1 AND 255),
    CONSTRAINT ck_jobs_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_jobs_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT ck_jobs_priority CHECK (priority BETWEEN 0 AND 3),
    CONSTRAINT ck_jobs_status CHECK (status IN
        ('PENDING', 'READY', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY_WAIT', 'TIMEOUT', 'CANCELLED')),
    CONSTRAINT ck_jobs_max_attempts CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT ck_jobs_attempt_count CHECK (attempt_count BETWEEN 0 AND max_attempts),
    CONSTRAINT ck_jobs_retry_delays CHECK
        (retry_initial_delay_ms >= 0 AND retry_max_delay_ms >= retry_initial_delay_ms),
    CONSTRAINT ck_jobs_retry_backoff CHECK (retry_backoff_multiplier BETWEEN 1 AND 100),
    CONSTRAINT ck_jobs_timeout CHECK (timeout_ms > 0),
    CONSTRAINT ck_jobs_idempotency_key CHECK
        (idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 255),
    CONSTRAINT ck_jobs_version CHECK (version >= 1),
    CONSTRAINT ck_jobs_updated_after_created CHECK (updated_at >= created_at),
    -- finished_at is set exactly while the job is in a final state.
    CONSTRAINT ck_jobs_finished_matches_status CHECK
        ((status IN ('COMPLETED', 'FAILED', 'TIMEOUT', 'CANCELLED')) = (finished_at IS NOT NULL)),
    CONSTRAINT ck_jobs_finished_after_created CHECK (finished_at IS NULL OR finished_at >= created_at)
);

-- Ready queue order: what the scheduler pops next and what it re-publishes to Redis after a flush.
CREATE INDEX idx_jobs_ready_queue ON jobs (priority DESC, scheduled_at, sequence)
    WHERE status = 'READY';

-- Jobs that become runnable by the passage of time (delayed jobs and retry backoff).
CREATE INDEX idx_jobs_due ON jobs (scheduled_at)
    WHERE status IN ('PENDING', 'RETRY_WAIT');

-- Newest-first listing for the future job API.
CREATE INDEX idx_jobs_created_at ON jobs (created_at DESC, id);

-- A job may only be created PENDING or READY.
CREATE FUNCTION taskflow_guard_job_initial_state() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status NOT IN ('PENDING', 'READY') THEN
        RAISE EXCEPTION 'a job must be created PENDING or READY, not %', NEW.status
            USING ERRCODE = 'TF001';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_jobs_initial_state
    BEFORE INSERT ON jobs
    FOR EACH ROW EXECUTE FUNCTION taskflow_guard_job_initial_state();

-- Enforces the job state machine in the database, so no writer (including a
-- future one in another language, or a manual psql session) can skip it.
-- Keep the table identical to src/jobs/job_state.cpp; an integration test
-- compares both for every pair of states.
CREATE FUNCTION taskflow_guard_job_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.sequence IS DISTINCT FROM OLD.sequence
       OR NEW.type IS DISTINCT FROM OLD.type
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'id, sequence, type, idempotency_key and created_at are immutable'
            USING ERRCODE = 'TF002';
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status THEN
        IF (OLD.status, NEW.status) NOT IN (
            ('PENDING',    'READY'),
            ('PENDING',    'CANCELLED'),
            ('READY',      'RUNNING'),
            ('READY',      'CANCELLED'),
            ('RUNNING',    'COMPLETED'),
            ('RUNNING',    'FAILED'),
            ('RUNNING',    'RETRY_WAIT'),
            ('RUNNING',    'TIMEOUT'),
            ('RUNNING',    'CANCELLED'),
            ('RETRY_WAIT', 'READY'),
            ('RETRY_WAIT', 'CANCELLED'),
            ('FAILED',     'READY'),
            ('TIMEOUT',    'READY')
        ) THEN
            RAISE EXCEPTION 'illegal job transition % -> %', OLD.status, NEW.status
                USING ERRCODE = 'TF001';
        END IF;
    END IF;

    NEW.version := OLD.version + 1;
    NEW.updated_at := GREATEST(clock_timestamp(), OLD.updated_at);
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_jobs_update_guard
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION taskflow_guard_job_update();
