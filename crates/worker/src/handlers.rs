//! Built-in, side-effect-free job kinds used for demos, tests and benchmarks.

use std::time::Duration;

use serde_json::{Value, json};

use crate::{HandlerError, HandlerRegistry, JobContext};

/// Longest a `sleep` job may ask for.
pub const MAX_SLEEP_MS: u64 = 60_000;

/// A registry with every built-in kind: `echo`, `sum`, `sleep`, `fail`.
pub fn builtin() -> HandlerRegistry {
    HandlerRegistry::new()
        .register("echo", echo)
        .register("sum", sum)
        .register("sleep", sleep)
        .register("fail", fail)
}

/// Returns its payload unchanged.
pub async fn echo(_ctx: JobContext, payload: Value) -> Result<Value, HandlerError> {
    Ok(payload)
}

/// `{"numbers": [1, 2, 3]}` -> `{"sum": 6.0, "count": 3}`.
pub async fn sum(_ctx: JobContext, payload: Value) -> Result<Value, HandlerError> {
    let numbers = payload
        .get("numbers")
        .and_then(Value::as_array)
        .ok_or_else(|| HandlerError::new("payload.numbers must be an array of numbers"))?;
    let mut total = 0.0;
    for (i, n) in numbers.iter().enumerate() {
        total += n
            .as_f64()
            .ok_or_else(|| HandlerError::new(format!("payload.numbers[{i}] is not a number")))?;
    }
    Ok(json!({ "sum": total, "count": numbers.len() }))
}

/// `{"ms": 250}` -> waits, then `{"slept_ms": 250}`. Stands in for I/O-bound work.
pub async fn sleep(_ctx: JobContext, payload: Value) -> Result<Value, HandlerError> {
    let ms = payload
        .get("ms")
        .and_then(Value::as_u64)
        .ok_or_else(|| HandlerError::new("payload.ms must be a non-negative integer"))?;
    if ms > MAX_SLEEP_MS {
        return Err(HandlerError::new(format!(
            "payload.ms must be at most {MAX_SLEEP_MS}"
        )));
    }
    tokio::time::sleep(Duration::from_millis(ms)).await;
    Ok(json!({ "slept_ms": ms }))
}

/// Always fails, with `payload.message` if given. For exercising failure paths.
pub async fn fail(ctx: JobContext, payload: Value) -> Result<Value, HandlerError> {
    let message = payload
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("deliberate failure");
    Err(HandlerError::new(format!(
        "{message} (attempt {})",
        ctx.attempt
    )))
}

#[cfg(test)]
mod tests {
    use taskforge_domain::JobId;

    use super::*;

    fn ctx(kind: &str) -> JobContext {
        JobContext {
            job_id: JobId::new(),
            kind: kind.to_owned(),
            queue: "default".to_owned(),
            attempt: 1,
            worker_id: "test".to_owned(),
        }
    }

    #[tokio::test]
    async fn echo_returns_the_payload() {
        let payload = json!({"a": [1, 2, {"b": null}]});
        assert_eq!(echo(ctx("echo"), payload.clone()).await, Ok(payload));
    }

    #[tokio::test]
    async fn sum_adds_and_validates() {
        assert_eq!(
            sum(ctx("sum"), json!({"numbers": [1, 2.5, -0.5]})).await,
            Ok(json!({"sum": 3.0, "count": 3}))
        );
        assert_eq!(
            sum(ctx("sum"), json!({"numbers": []})).await,
            Ok(json!({"sum": 0.0, "count": 0}))
        );
        assert!(sum(ctx("sum"), json!({})).await.is_err());
        let err = sum(ctx("sum"), json!({"numbers": [1, "two"]}))
            .await
            .unwrap_err();
        assert!(err.message().contains("numbers[1]"));
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_waits_and_is_bounded() {
        let started = tokio::time::Instant::now();
        assert_eq!(
            sleep(ctx("sleep"), json!({"ms": 250})).await,
            Ok(json!({"slept_ms": 250}))
        );
        assert_eq!(started.elapsed(), Duration::from_millis(250));

        assert!(
            sleep(ctx("sleep"), json!({"ms": MAX_SLEEP_MS + 1}))
                .await
                .is_err()
        );
        assert!(sleep(ctx("sleep"), json!({"ms": -1})).await.is_err());
    }

    #[tokio::test]
    async fn fail_always_fails() {
        let err = fail(ctx("fail"), json!({"message": "boom"}))
            .await
            .unwrap_err();
        assert_eq!(err.message(), "boom (attempt 1)");
    }

    #[test]
    fn builtin_registry_lists_exactly_the_safe_kinds() {
        let kinds: Vec<_> = builtin().kinds().into_iter().collect();
        assert_eq!(kinds, ["echo", "fail", "sleep", "sum"]);
    }
}
