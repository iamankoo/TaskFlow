-- Phase 2: one row per execution attempt of a job.

CREATE TABLE job_attempts (
    id              bigint      GENERATED ALWAYS AS IDENTITY,
    job_id          uuid        NOT NULL,
    attempt_number  integer     NOT NULL,
    status          text        NOT NULL DEFAULT 'RUNNING',
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    error_message   text,

    CONSTRAINT pk_job_attempts PRIMARY KEY (id),
    CONSTRAINT fk_job_attempts_job FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    CONSTRAINT uq_job_attempts_job_number UNIQUE (job_id, attempt_number),
    -- Target of the composite foreign key from job_results (keeps result and attempt on the same job).
    CONSTRAINT uq_job_attempts_id_job UNIQUE (id, job_id),

    CONSTRAINT ck_job_attempts_number CHECK (attempt_number >= 1),
    CONSTRAINT ck_job_attempts_status CHECK
        (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED')),
    CONSTRAINT ck_job_attempts_finished_matches_status CHECK
        ((status = 'RUNNING') = (finished_at IS NULL)),
    CONSTRAINT ck_job_attempts_finished_after_started CHECK
        (finished_at IS NULL OR finished_at >= started_at),
    -- Only unsuccessful, finished attempts carry an error message.
    CONSTRAINT ck_job_attempts_error_message CHECK
        (status IN ('FAILED', 'TIMED_OUT', 'CANCELLED') OR error_message IS NULL)
);

-- A job has at most one active attempt, no matter who writes.
CREATE UNIQUE INDEX uq_job_attempts_one_running ON job_attempts (job_id) WHERE status = 'RUNNING';
