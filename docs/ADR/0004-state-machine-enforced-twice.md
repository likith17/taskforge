# ADR 0004 — Enforce the state machine in the domain *and* in the database

Status: accepted · Milestone 1

## Context

"Prevent invalid state transitions" can be implemented in application code
alone. But the failure being guarded against is a bug in application code — a
missing `WHERE` clause in a future query, a hand-run `UPDATE` during an
incident, a second service writing to the table.

## Options

1. **Application only.** One definition, easy to change. A single wrong query
   silently corrupts state.
2. **Database only** (trigger/constraints). Unbypassable, but the rules live
   in PL/pgSQL where the rest of the code cannot use them for decisions or
   tests.
3. **Both, with a test that they agree.**

## Decision

Option 3.

- `JobState::can_transition_to` in `taskforge-domain` is the readable source
  of truth, unit-tested against the documented table.
- The trigger `jobs_enforce_transition` rejects any `UPDATE` that changes
  `state` along an edge not in the table, any insert that is not
  `queued`/attempt 0, and any decrease of the fencing token.
- CHECK constraints tie the lease columns to `running`, `finished_at` to
  terminal states, and `result` to `succeeded`.
- The integration test
  `database_trigger_enforces_exactly_the_domain_state_machine` attempts all 20
  transitions between distinct states with raw SQL and asserts the database's
  verdict equals the domain function's.

## Consequences

- The rule exists in two places. The agreement test turns "kept in sync by
  discipline" into "kept in sync by CI": changing one without the other fails
  the build.
- Normal operation never hits the trigger's error path, because every
  application `UPDATE` is already guarded; the trigger costs one small
  function call per row update. Its overhead will be measured in the
  benchmarks rather than assumed negligible.
- State is stored as `text` with a CHECK rather than a PostgreSQL `enum`:
  adding a state is then an ordinary transactional migration.
