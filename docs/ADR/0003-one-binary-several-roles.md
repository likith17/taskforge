# ADR 0003 — One binary with roles, not separate services

Status: accepted · Milestone 1

## Context

The system has an HTTP API, workers, and (from Milestone 2) a scheduler loop
that recovers expired leases. They could be one process, separate services, or
something in between.

## Options

1. **Monolith, single process only.** Simplest, but API and workers cannot be
   scaled or restarted independently, and a handler that exhausts memory takes
   the API down with it.
2. **Separate services** (separate binaries, images, release cycles). Maximum
   isolation; also three artefacts to build, version and deploy, and a schema
   shared between independently released binaries.
3. **One binary, role chosen at start-up** (`taskforge serve`,
   `taskforge worker`, `taskforge all`). One artefact and one version, but
   each role can run as its own process and scale separately.

## Decision

Option 3. The code is split into library crates along component lines
(`api`, `worker`, `storage`, `domain`), and a thin binary crate selects which
of them to run.

`taskforge all` runs the API and one worker in a single process for
development and demos. Production-style deployments run `serve` and `worker`
as separate processes from the same image.

## Consequences

- A single version of the schema and code is always deployed together; there
  is no cross-service compatibility matrix.
- Roles scale independently: more `worker` processes for throughput, more
  `serve` processes for request volume.
- Only the `serve`/`migrate` roles apply migrations. Workers never do, so they
  can later run with a database role that has no DDL rights.
- The modular crates keep the option of splitting into real services open, but
  nothing currently justifies paying for it.

## Related decision: workers talk to PostgreSQL directly

The Python prototype routed workers through the HTTP API so that they held no
database credentials. That added a network hop and a second failure mode to
every claim, heartbeat and completion, and required making each worker
endpoint idempotent to survive lost responses.

TaskForge workers use the database directly, as Sidekiq and Celery workers use
their stores. The fenced SQL statements are the protocol. The cost is that
workers need database access, which is acceptable for a system whose workers
run inside the same trust boundary; restricting what the worker role may do
(DML on `jobs` only) is planned as part of least-privilege credentials.
