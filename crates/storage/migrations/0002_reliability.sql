-- Milestone 2 schema: retries, idempotency, cancellation, attempt history.

ALTER TABLE jobs
    -- Attempts that ended badly. Distinct from `attempt` (deliveries): a
    -- graceful release is a delivery but not a failure.
    ADD COLUMN failures         integer NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts     integer NOT NULL DEFAULT 5,
    ADD COLUMN idempotency_key  text,
    ADD COLUMN cancel_requested boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT jobs_failures_non_negative CHECK (failures >= 0),
    ADD CONSTRAINT jobs_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 25),
    ADD CONSTRAINT jobs_idempotency_key_length
        CHECK (idempotency_key IS NULL OR length(idempotency_key) BETWEEN 1 AND 255);

-- One job per idempotency key. This index, not application code, is what makes
-- a duplicate submission impossible: concurrent inserts with the same key
-- collide here and all but one are turned away.
CREATE UNIQUE INDEX jobs_idempotency_key_uq
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Listing by state, newest first (ids are UUIDv7, so id order is time order).
CREATE INDEX jobs_state_idx ON jobs (state, id DESC);

-- One row per delivery of a job to a worker. `outcome` is NULL while the
-- attempt is in flight. More than one row for a job means it was executed
-- more than once: this table is where at-least-once delivery becomes visible.
CREATE TABLE job_attempts (
    job_id      uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     integer     NOT NULL,
    worker_id   text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text,
    error       text,
    PRIMARY KEY (job_id, attempt),
    CONSTRAINT job_attempts_outcome_valid CHECK (
        outcome IN ('succeeded', 'failed', 'lease_expired', 'cancelled', 'released')
    ),
    CONSTRAINT job_attempts_finished_iff_outcome
        CHECK ((outcome IS NULL) = (finished_at IS NULL))
);
