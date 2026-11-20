//! The `chaos` job kind, used by the chaos and benchmark harness
//! (`taskforge-chaos`). Registered only when `TASKFORGE_CHAOS_HANDLER=true`.
//!
//! Each run writes two rows to `chaos_ledger`, a table the harness creates:
//!
//! * `start`  - the handler began executing this attempt;
//! * `finish` - the handler's work is done (written just before returning).
//!
//! The ledger is deliberately *not* idempotent. It is the stand-in for a
//! side effect with no protection, so it shows exactly how often at-least-once
//! delivery really re-ran work:
//!
//! * jobs with more than one `start` were executed more than once;
//! * jobs with more than one `finish` would have performed an unprotected
//!   side effect twice (a worker died after the work but before recording it).
//!
//! The harness compares that with `job_attempts`, where every job must have
//! exactly one `succeeded` attempt.

use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;
use taskforge_worker::{HandlerError, HandlerRegistry, JobContext};

const MAX_WORK_MS: u64 = 60_000;

pub fn register(registry: HandlerRegistry, pool: PgPool) -> HandlerRegistry {
    registry.register("chaos", move |ctx: JobContext, payload: Value| {
        let pool = pool.clone();
        async move {
            let work_ms = payload
                .get("work_ms")
                .and_then(Value::as_u64)
                .unwrap_or(100)
                .min(MAX_WORK_MS);
            record(&pool, &ctx, "start").await?;
            tokio::time::sleep(Duration::from_millis(work_ms)).await;
            if payload.get("fail").and_then(Value::as_bool) == Some(true) {
                return Err(HandlerError::new("deliberate failure (chaos job)"));
            }
            record(&pool, &ctx, "finish").await?;
            Ok(json!({ "n": payload.get("n").cloned().unwrap_or(Value::Null) }))
        }
    })
}

async fn record(pool: &PgPool, ctx: &JobContext, event: &str) -> Result<(), HandlerError> {
    sqlx::query(
        "INSERT INTO chaos_ledger (job_id, attempt, worker_id, event) VALUES ($1, $2, $3, $4)",
    )
    .bind(ctx.job_id.as_uuid())
    .bind(ctx.attempt)
    .bind(&ctx.worker_id)
    .bind(event)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| HandlerError::new(format!("chaos ledger write failed: {e}")))
}
