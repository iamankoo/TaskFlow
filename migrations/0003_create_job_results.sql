-- Phase 2: the outcome of an attempt, stored apart from the job row so large
-- outputs never bloat the hot jobs table and results can be read on their own.

CREATE TABLE job_results (
    id             bigint      GENERATED ALWAYS AS IDENTITY,
    job_id         uuid        NOT NULL,
    attempt_id     bigint      NOT NULL,
    success        boolean     NOT NULL,
    output         jsonb,
    error_message  text,
    completed_at   timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_job_results PRIMARY KEY (id),
    -- At most one result per attempt.
    CONSTRAINT uq_job_results_attempt UNIQUE (attempt_id),
    CONSTRAINT fk_job_results_job FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    -- The attempt must belong to the same job as the result.
    CONSTRAINT fk_job_results_attempt FOREIGN KEY (attempt_id, job_id)
        REFERENCES job_attempts (id, job_id) ON DELETE CASCADE,

    -- Success carries output and no error; failure carries an error and no output.
    CONSTRAINT ck_job_results_shape CHECK (
        (success AND error_message IS NULL)
        OR (NOT success AND error_message IS NOT NULL AND output IS NULL))
);

-- Latest result for a job; also serves the foreign-key lookup on job_id.
CREATE INDEX idx_job_results_job ON job_results (job_id, id DESC);
