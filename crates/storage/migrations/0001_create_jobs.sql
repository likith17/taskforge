-- Milestone 1 schema: the durable queue.
--
-- The table is the queue. A job's existence and its status live in the same
-- row, so they can never disagree, and every state change is one UPDATE.

CREATE TABLE jobs (
    id               uuid        PRIMARY KEY,
    queue            text        NOT NULL,
    kind             text        NOT NULL,
    payload          jsonb       NOT NULL,
    state            text        NOT NULL DEFAULT 'queued',
    priority         smallint    NOT NULL DEFAULT 0,
    run_at           timestamptz NOT NULL DEFAULT now(),

    -- Number of times the job has been claimed. Also the fencing token: a
    -- worker may only settle the attempt it was handed.
    attempt          integer     NOT NULL DEFAULT 0,
    lease_owner      text,
    lease_expires_at timestamptz,

    result           jsonb,
    last_error       text,

    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz,

    CONSTRAINT jobs_state_valid
        CHECK (state IN ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
    CONSTRAINT jobs_priority_range
        CHECK (priority BETWEEN -100 AND 100),
    CONSTRAINT jobs_attempt_non_negative
        CHECK (attempt >= 0),
    -- A lease is both fields or neither ...
    CONSTRAINT jobs_lease_fields_together
        CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    -- ... and a job holds a lease exactly while it is running.
    CONSTRAINT jobs_lease_iff_running
        CHECK ((state = 'running') = (lease_owner IS NOT NULL)),
    CONSTRAINT jobs_finished_iff_terminal
        CHECK ((state IN ('succeeded', 'dead', 'cancelled')) = (finished_at IS NOT NULL)),
    -- A result exists only for a job that succeeded.
    CONSTRAINT jobs_result_only_when_succeeded
        CHECK (result IS NULL OR state = 'succeeded')
);

-- Claim path: claimable jobs of a queue, in dispatch order. Partial, so it
-- stays small however many finished jobs accumulate.
CREATE INDEX jobs_claim_idx
    ON jobs (queue, priority DESC, run_at, id)
    WHERE state = 'queued';

-- Lease-recovery path (used from milestone 2): running jobs by lease expiry.
CREATE INDEX jobs_lease_idx
    ON jobs (lease_expires_at)
    WHERE state = 'running';

-- Second line of defence for the state machine. Application code only issues
-- guarded UPDATEs, but if a bug (or a human with psql) tries an illegal
-- transition, the database refuses it. The table below must stay identical to
-- `JobState::can_transition_to` in the domain crate; a test compares them.
CREATE FUNCTION jobs_enforce_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'queued' OR NEW.attempt <> 0 THEN
            RAISE EXCEPTION 'a job must be inserted as queued with attempt 0'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
           (OLD.state = 'queued'  AND NEW.state IN ('running', 'cancelled'))
        OR (OLD.state = 'running' AND NEW.state IN ('succeeded', 'queued', 'dead', 'cancelled'))
        OR (OLD.state = 'dead'    AND NEW.state = 'queued')
    ) THEN
        RAISE EXCEPTION 'invalid job state transition: % -> %', OLD.state, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;

    -- The fencing token only ever moves forward.
    IF NEW.attempt < OLD.attempt THEN
        RAISE EXCEPTION 'job attempt must not decrease: % -> %', OLD.attempt, NEW.attempt
            USING ERRCODE = 'check_violation';
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END
$$;

CREATE TRIGGER jobs_enforce_transition
    BEFORE INSERT OR UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION jobs_enforce_transition();
