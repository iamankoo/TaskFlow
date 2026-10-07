-- Phase 2: append-only audit trail of job state changes, written in the same
-- transaction as the change it records.

CREATE TABLE job_events (
    id           bigint      GENERATED ALWAYS AS IDENTITY,
    job_id       uuid        NOT NULL,
    from_status  text,
    to_status    text        NOT NULL,
    reason       text,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_job_events PRIMARY KEY (id),
    CONSTRAINT fk_job_events_job FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    CONSTRAINT ck_job_events_from CHECK (from_status IS NULL OR from_status IN
        ('PENDING', 'READY', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY_WAIT', 'TIMEOUT', 'CANCELLED')),
    CONSTRAINT ck_job_events_to CHECK (to_status IN
        ('PENDING', 'READY', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY_WAIT', 'TIMEOUT', 'CANCELLED')),
    -- from_status is NULL only for the creation event.
    CONSTRAINT ck_job_events_moved CHECK (from_status IS NULL OR from_status <> to_status)
);

CREATE INDEX idx_job_events_job ON job_events (job_id, id);

-- Events are never edited. (DELETE is allowed so that removing a job cascades its history.)
CREATE FUNCTION taskflow_job_events_no_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'job_events is append-only' USING ERRCODE = 'TF003';
END
$$;

CREATE TRIGGER trg_job_events_no_update
    BEFORE UPDATE ON job_events
    FOR EACH ROW EXECUTE FUNCTION taskflow_job_events_no_update();
