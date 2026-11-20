# TaskForge

A fault-tolerant background job platform in Rust, built on PostgreSQL —
inspired by the engineering behind Celery, Sidekiq and Temporal, and
deliberately much smaller.

**Status: Milestone 1 of 4 — a working vertical slice.** Jobs can be
submitted over HTTP, are stored durably, and are claimed and executed by a
worker. Retries, crash recovery, idempotency keys, metrics and the dashboard
are not built yet; see [docs/PLAN.md](docs/PLAN.md) for what is and is not
done, and [docs/FAILURE_ANALYSIS.md](docs/FAILURE_ANALYSIS.md) for the known
gaps.

## What works today

- `POST /jobs` and `GET /jobs/{id}`, with validation and a consistent JSON
  error format
- A durable PostgreSQL-backed queue: an accepted job survives a process kill
- Atomic claiming with `FOR UPDATE SKIP LOCKED` — concurrent workers never get
  the same job
- Fencing tokens: a worker can only record the outcome of the attempt it holds
- A job state machine enforced both in Rust and by a database trigger
- Priorities and delayed execution
- Only predefined, registered handlers run (`echo`, `sum`, `sleep`, `fail`)
- 50 automated tests, including concurrency and database-outage cases

## What it promises (and what it does not)

Execution is **at-least-once**, not exactly-once: a worker can finish a job
and die before recording it. What TaskForge guarantees is that an accepted job
is durable, is leased to one attempt at a time, and gets at most one committed
result. Details in [docs/ARCHITECTURE.md §5](docs/ARCHITECTURE.md).

At Milestone 1 a job whose worker dies mid-run stays `running` until recovered
by hand — automatic recovery is the first item of Milestone 2.

## Quick start

Requires Rust 1.85+ and Docker.

```bash
docker compose up -d db
```

```bash
cargo run -p taskforge -- all
```

That starts the API on `127.0.0.1:8080` plus one worker, applying migrations
first. In another terminal:

```bash
curl -i -X POST http://127.0.0.1:8080/jobs -H "content-type: application/json" -d '{"kind":"sum","payload":{"numbers":[40,2]}}'
```

Take the `id` from the response (or the `Location` header) and fetch it:

```bash
curl http://127.0.0.1:8080/jobs/<id>
```

```json
{ "state": "succeeded", "attempt": 1, "result": { "count": 2, "sum": 42.0 }, "...": "..." }
```

Things to try:

| Request body | What happens |
|---|---|
| `{"kind":"echo","payload":{"hi":1},"delay_ms":5000}` | Stays `queued` for 5 s, then runs. |
| `{"kind":"fail","payload":{"message":"boom"}}` | Ends `dead` with `last_error`. |
| `{"kind":"shell","payload":{"cmd":"whoami"}}` | `422` — no such handler; nothing is queued. |
| `{"kind":"echo","priority":101}` | `422` naming the `priority` field. |

**Durability check:** submit a job with `"delay_ms": 10000`, kill the process
(Ctrl+C, or harder), start it again, and fetch the job: it is still `queued`,
then runs.

API and worker can also run as separate processes: `taskforge serve` and
`taskforge worker` (plus `taskforge migrate` to apply the schema only).
Full API reference: [docs/API.md](docs/API.md).

## Configuration

Environment variables; see [.env.example](.env.example).

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | — (required) | `cargo run`/`cargo test` default to the Compose database via `.cargo/config.toml`. |
| `TASKFORGE_BIND` | `127.0.0.1:8080` | Loopback by default: there is no authentication yet. |
| `TASKFORGE_QUEUES` | `default` | Comma-separated queues the worker serves. |
| `TASKFORGE_POLL_INTERVAL_MS` | `250` | Idle polling interval. |
| `TASKFORGE_LEASE_SECS` | `300` | Lease written on claim. |
| `TASKFORGE_MAX_BODY_BYTES` | `66560` | Request body limit. |
| `TASKFORGE_LOG_FORMAT` | `text` | `json` for structured logs. |
| `RUST_LOG` | `info` | Log filter. |

## Development

```bash
cargo test --workspace
```

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

```bash
cargo fmt --all --check
```

Integration tests need the Compose database running. Each test gets its own
temporary database, so they run in parallel and leave nothing behind.

## Layout

```
crates/
  domain/     pure rules: job states, transitions, validation (no I/O)
  storage/    every SQL statement; migrations; the fenced claim/settle protocol
  api/        Axum router and error handling
  worker/     handler registry, built-in handlers, the worker loop
  taskforge/  the binary: config, logging, signals, role selection
docs/
  PROJECT_BRIEF.md   problem, stakeholders, success metrics
  ARCHITECTURE.md    components, state machine, schema, delivery semantics
  ADR/               decision records (Postgres vs Redis, polling vs notify, ...)
  DECISIONS.md       decision index and smaller choices
  FAILURE_ANALYSIS.md  failure modes, known gaps, approaches that did not work
  BENCHMARKS.md      benchmark methodology (no results yet)
  PLAN.md            milestones and progress
  API.md             HTTP reference
```

## Security notes

Only handlers compiled into the binary can run; a job is a name plus JSON,
never code. There is **no authentication, authorization or TLS** yet, which is
why the API binds to loopback. Do not expose it to a network as it stands.
