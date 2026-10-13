-- Phase 4: job dependencies (a directed acyclic graph of jobs).
-- An edge (job_id, depends_on_id) means "job_id must not run until depends_on_id COMPLETED".
-- Forward-only: this migration adds a table and REPLACES one trigger function; it changes no row
-- of any existing table, so databases that already applied 0001-0004 upgrade in place.
-- See docs/DATABASE.md ("Dependencies") for the design notes.

CREATE TABLE job_dependencies (
    job_id         uuid        NOT NULL,   -- the dependent: waits
    depends_on_id  uuid        NOT NULL,   -- the prerequisite: must COMPLETE first
    created_at     timestamptz NOT NULL DEFAULT now(),

    -- The primary key rejects a duplicate edge and serves "prerequisites of a job".
    CONSTRAINT pk_job_dependencies PRIMARY KEY (job_id, depends_on_id),
    -- Deleting a dependent removes its edges.
    CONSTRAINT fk_job_dependencies_job FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
    -- A job that others still wait for cannot be deleted from under them. The check is deferred
    -- to COMMIT: when a whole workflow is deleted at once, the cascade from each dependent has
    -- removed its edges by then, whatever order the rows were deleted in.
    CONSTRAINT fk_job_dependencies_prerequisite FOREIGN KEY (depends_on_id) REFERENCES jobs (id)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_job_dependencies_no_self CHECK (job_id <> depends_on_id)
);

-- "Dependents of a job": which jobs wait for this one (resolution after a state change).
CREATE INDEX idx_job_dependencies_prerequisite ON job_dependencies (depends_on_id, job_id);

-- Edges are immutable and may only be added to a job that has not been released yet.
CREATE FUNCTION taskflow_guard_job_dependency() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    dependent_status text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'job_dependencies edges are immutable' USING ERRCODE = 'TF003';
    END IF;
    SELECT status INTO dependent_status FROM jobs WHERE id = NEW.job_id;
    IF dependent_status IS DISTINCT FROM 'PENDING' THEN
        RAISE EXCEPTION 'prerequisites can only be added to a PENDING job, but job % is %',
            NEW.job_id, dependent_status USING ERRCODE = 'TF004';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_job_dependencies_guard
    BEFORE INSERT OR UPDATE ON job_dependencies
    FOR EACH ROW EXECUTE FUNCTION taskflow_guard_job_dependency();

-- Replaces the Phase 2 function (same trigger, new body). Two changes:
--   1. PENDING -> FAILED is now legal (a job whose prerequisite failed can never run).
--   2. A job can only become READY when every prerequisite is COMPLETED, whoever asks:
--      the scheduler, an operator, a future API, or a stale Redis entry cannot bypass it.
CREATE OR REPLACE FUNCTION taskflow_guard_job_update() RETURNS trigger
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
            ('PENDING',    'FAILED'),
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

        IF NEW.status = 'READY' AND EXISTS (
            SELECT 1
            FROM job_dependencies d
            JOIN jobs p ON p.id = d.depends_on_id
            WHERE d.job_id = NEW.id AND p.status <> 'COMPLETED'
        ) THEN
            RAISE EXCEPTION 'job % cannot become READY: it has prerequisites that are not COMPLETED', NEW.id
                USING ERRCODE = 'TF004';
        END IF;
    END IF;

    NEW.version := OLD.version + 1;
    NEW.updated_at := GREATEST(clock_timestamp(), OLD.updated_at);
    RETURN NEW;
END
$$;
