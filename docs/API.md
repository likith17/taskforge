# HTTP API

Base URL in examples: `http://127.0.0.1:8080`. Bodies are JSON. There is **no
authentication**; see README, Known limitations.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job (`Idempotency-Key` supported) |
| `GET` | `/jobs` | List jobs, newest first |
| `GET` | `/jobs/{id}` | One job: state, result or last error |
| `GET` | `/jobs/{id}/attempts` | Delivery history |
| `POST` | `/jobs/{id}/cancel` | Cancel a queued job, or ask a running one to stop |
| `POST` | `/jobs/{id}/retry` | Re-drive a dead-lettered job |
| `GET` | `/metrics` | Prometheus metrics (OpenMetrics text) |
| `GET` | `/healthz` | Liveness: the process is up |
| `GET` | `/readyz` | Readiness: the database answers |

## `POST /jobs`

| Field | Type | Default | Notes |
|---|---|---|---|
| `kind` | string | required | A registered kind: `echo`, `sum`, `sleep`, `fail` (and `chaos` when enabled) |
| `payload` | JSON | `{}` | At most 64 KiB |
| `queue` | string | `"default"` | |
| `priority` | integer | `0` | −100..100, higher runs first |
| `run_at` | RFC 3339 | now | Earliest start |
| `delay_ms` | integer | — | Relative alternative to `run_at`, at most 365 days |
| `max_attempts` | integer | `5` | 1..25 failed attempts before dead-lettering |
| `timeout_secs` | integer | `300` | 1..86400; a longer attempt is stopped and fails (retryably) |

Names are 1–64 characters: a lowercase letter, then lowercase letters, digits,
`_`, `-` or `.`. Unknown fields are rejected.

Header `Idempotency-Key` (1–255 printable ASCII, no spaces) makes the request
safe to repeat:

| Situation | Response |
|---|---|
| First request with this key | `201 Created`, `Location: /jobs/{id}` |
| Same key, same request | `200 OK`, `Idempotent-Replay: true`, the original job |
| Same key, different request | `409`, `idempotency_key_reused` |

"Same request" compares a fingerprint of `kind`, `queue`, `payload`, `priority`,
`max_attempts` and `timeout_secs`. The schedule is excluded, because a relative
`delay_ms` resolves differently on every retry. Keys are honoured until the job is
purged (7 days after it finishes, by default).

```bash
curl -i -X POST http://127.0.0.1:8080/jobs -H "content-type: application/json" -H "Idempotency-Key: order-8841" -d '{"kind":"sum","payload":{"numbers":[40,2]},"max_attempts":8}'
```

## `GET /jobs/{id}`

Returns the job:

```json
{
  "id": "01a1e852-6c7e-715a-827e-116b9c2f2582",
  "queue": "default", "kind": "sum", "payload": { "numbers": [40, 2] },
  "state": "succeeded", "priority": 0, "run_at": "...",
  "attempt": 1, "failures": 0, "max_attempts": 8, "timeout_secs": 300,
  "idempotency_key": "order-8841", "cancel_requested": false,
  "lease_owner": null, "lease_expires_at": null,
  "result": { "count": 2, "sum": 42.0 }, "last_error": null,
  "created_at": "...", "updated_at": "...", "started_at": "...", "finished_at": "..."
}
```

`state` is one of `queued`, `running`, `succeeded`, `dead`, `cancelled`.
`attempt` counts deliveries; `failures` counts deliveries that ended badly.

## `GET /jobs/{id}/attempts`

```json
{ "attempts": [
  { "attempt": 1, "worker_id": "w2-g5", "started_at": "...", "finished_at": "...", "outcome": "lease_expired", "error": "lease expired: the worker stopped renewing it" },
  { "attempt": 2, "worker_id": "w1-g1", "started_at": "...", "finished_at": "...", "outcome": "succeeded", "error": null }
] }
```

`outcome` is `succeeded`, `failed`, `lease_expired`, `cancelled`, `released`, or
`null` while in flight. More than one entry means the job was executed more than
once.

## `GET /jobs`

Query parameters: `state`, `queue`, `kind`, `limit` (1–200, default 50), and
`before` (cursor). The response is `{"jobs": [...], "next_before": "<id>" | null}`.
To get the next page, pass `next_before` as `before`.

## `POST /jobs/{id}/cancel`

| Job was | Response |
|---|---|
| `queued` | `200`, job is `cancelled` and never runs |
| `running` | `202`, `cancel_requested: true`. The worker sees it on its next heartbeat and stops the handler. If the handler's result is committed first, the job ends `succeeded`. |
| `succeeded` / `dead` | `409`, `already_finished` |
| `cancelled` | `200` (repeating is harmless) |

## `POST /jobs/{id}/retry`

Moves a `dead` job back to `queued` with `failures` reset. History is kept and
the fencing token keeps counting. Any other state gets `409 invalid_state`.

## `GET /metrics`

| Metric | Type | Meaning |
|---|---|---|
| `taskforge_worker_job_outcomes_total{outcome}` | counter | `succeeded`, `retrying`, `dead_lettered`, `cancelled`, `released`, `lost_claim`, `unreported` |
| `taskforge_lease_expirations_total{result}` | counter | Leases this process took back: `requeued`, `dead_lettered`, `cancelled` |
| `taskforge_submissions_total{result}` | counter | `created`, `duplicate` |
| `taskforge_jobs_purged_total` | counter | Finished jobs deleted after retention |
| `taskforge_heartbeat_failures_total` | counter | Renewals that could not reach the database |
| `taskforge_claim_duration_seconds` | histogram | One claim query, including empty polls |
| `taskforge_queue_wait_seconds` | histogram | Due to claimed, on the database clock |
| `taskforge_job_execution_seconds` | histogram | Handler run time |
| `taskforge_jobs{queue,state}` | gauge | Jobs in the database, read at scrape time |
| `taskforge_oldest_due_job_age_seconds` | gauge | Backlog age; growing means workers are behind |
| `taskforge_expired_unreaped_leases` | gauge | Persistently above 0 means no reaper is running |

Counters are per process. Worker and scheduler processes serve their own
`/metrics` when `TASKFORGE_METRICS_BIND` is set.

## Errors

```json
{ "error": { "code": "validation_failed", "field": "priority", "message": "must be between -100 and 100" } }
```

| Status | `code` | When |
|---|---|---|
| 400 | `bad_request` | Malformed JSON or query; id is not a UUID |
| 404 | `not_found` | No such job or route |
| 409 | `idempotency_key_reused`, `already_finished`, `invalid_state` | See above |
| 413 | `payload_too_large` | Body over the limit (default 65 KiB) |
| 415 | `unsupported_media_type` | Missing `content-type: application/json` |
| 422 | `validation_failed` | Out of range, unknown field, or unregistered `kind`; `field` names the culprit |
| 500 | `internal` | Unexpected; details are logged, not returned |
| 503 | `unavailable` | Database unreachable; sent with `Retry-After: 1` |
