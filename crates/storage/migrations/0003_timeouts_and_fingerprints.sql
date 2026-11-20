-- Per-job execution timeout, request fingerprints for idempotency checks,
-- and an index for purging finished jobs.

ALTER TABLE jobs
    ADD COLUMN timeout_secs integer NOT NULL DEFAULT 300,
    -- SHA-256 (hex) of the canonical submission. A repeated idempotency key
    -- is compared against this, not against the stored payload, because
    -- jsonb normalises numbers and so does not round-trip the request exactly.
    -- NULL for jobs created before this migration.
    ADD COLUMN request_hash text,
    ADD CONSTRAINT jobs_timeout_range CHECK (timeout_secs BETWEEN 1 AND 86400),
    ADD CONSTRAINT jobs_request_hash_format
        CHECK (request_hash IS NULL OR request_hash ~ '^[0-9a-f]{64}$');

-- Purge path: finished jobs, oldest first. `finished_at` is set exactly in
-- terminal states (see jobs_finished_iff_terminal), so this index only ever
-- holds jobs that are eligible for purging.
CREATE INDEX jobs_finished_idx ON jobs (finished_at) WHERE finished_at IS NOT NULL;
