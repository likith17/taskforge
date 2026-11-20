# HTTP API (Milestone 1)

Base URL in examples: `http://127.0.0.1:8080`. All bodies are JSON.
There is **no authentication** yet; see FAILURE_ANALYSIS §2.

## `POST /jobs` — submit a job

Request body:

| Field | Type | Required | Notes |
|---|---|---|---|
| `kind` | string | yes | A registered job kind: `echo`, `sum`, `sleep`, `fail`. |
| `payload` | any JSON | no | Default `{}`. At most 64 KiB. |
| `queue` | string | no | Default `"default"`. |
| `priority` | integer | no | −100..100, default 0. Higher runs first. |
| `run_at` | RFC 3339 timestamp | no | Earliest start. A past time means now. |
| `delay_ms` | integer | no | Earliest start, relative. Not together with `run_at`. Max 365 days. |

Names (`kind`, `queue`) are 1–64 characters: a lowercase letter, then
lowercase letters, digits, `_`, `-` or `.`. Unknown fields are rejected.

```bash
curl -i -X POST http://127.0.0.1:8080/jobs -H "content-type: application/json" -d '{"kind":"sum","payload":{"numbers":[40,2]},"priority":5}'
```

`201 Created`, with `Location: /jobs/{id}` and the job as the body:

```json
{
  "id": "01a1e852-6c7e-715a-827e-116b9c2f2582",
  "queue": "default",
  "kind": "sum",
  "payload": { "numbers": [40, 2] },
  "state": "queued",
  "priority": 5,
  "run_at": "2026-11-17T07:04:37.310Z",
  "attempt": 0,
  "lease_owner": null,
  "lease_expires_at": null,
  "result": null,
  "last_error": null,
  "created_at": "2026-11-17T07:04:37.310Z",
  "updated_at": "2026-11-17T07:04:37.310Z",
  "started_at": null,
  "finished_at": null
}
```

Once `201` is returned the job is durable. Submitting the same body twice
creates two jobs (idempotency keys arrive in Milestone 2).

## `GET /jobs/{id}` — job status

```bash
curl http://127.0.0.1:8080/jobs/01a1e852-6c7e-715a-827e-116b9c2f2582
```

`200 OK` with the job. `state` is one of `queued`, `running`, `succeeded`,
`dead`, `cancelled`. A succeeded job has `result`; a dead job has
`last_error`.

## `GET /healthz`, `GET /readyz`

`/healthz` answers `200` whenever the process is running. `/readyz` answers
`200` only if the database responds, otherwise `503`.

## Built-in job kinds

| Kind | Payload | Result |
|---|---|---|
| `echo` | anything | the payload |
| `sum` | `{"numbers": [1, 2.5]}` | `{"sum": 3.5, "count": 2}` |
| `sleep` | `{"ms": 250}` (max 60000) | `{"slept_ms": 250}` |
| `fail` | `{"message": "boom"}` (optional) | always fails |

## Errors

Every error has the same shape:

```json
{ "error": { "code": "validation_failed", "field": "priority", "message": "must be between -100 and 100" } }
```

| Status | `code` | When |
|---|---|---|
| 400 | `bad_request` | Body is not valid JSON; job id is not a UUID. |
| 404 | `not_found` | No such job or route. |
| 413 | `payload_too_large` | Body larger than the limit (default 65 KiB). |
| 415 | `unsupported_media_type` | Missing `content-type: application/json`. |
| 422 | `validation_failed` | Wrong shape or a value out of range; `field` names the culprit where known. Includes unknown job kinds. |
| 500 | `internal` | Unexpected server error. Details are logged, not returned. |
| 503 | `unavailable` | Database unreachable. Sent with `Retry-After: 1`. |
