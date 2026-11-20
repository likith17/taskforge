//! HTTP contract tests: the real router against a real (per-test) database.

use std::collections::BTreeSet;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use taskforge_api::{ApiConfig, router};
use taskforge_storage::JobStore;
use tower::ServiceExt;

fn app(pool: PgPool) -> Router {
    let known_kinds = BTreeSet::from(["echo".to_owned(), "sum".to_owned()]);
    router(
        JobStore::new(pool),
        ApiConfig {
            known_kinds: Some(known_kinds),
            ..ApiConfig::default()
        },
    )
}

struct Reply {
    status: StatusCode,
    location: Option<String>,
    retry_after: Option<String>,
    body: Value,
}

async fn send(app: &Router, request: Request<Body>) -> Reply {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let header_text = |name: header::HeaderName| {
        response
            .headers()
            .get(name)
            .map(|v: &header::HeaderValue| v.to_str().unwrap().to_owned())
    };
    let location = header_text(header::LOCATION);
    let retry_after = header_text(header::RETRY_AFTER);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("non-JSON body: {:?}", String::from_utf8_lossy(&bytes)));
    Reply {
        status,
        location,
        retry_after,
        body,
    }
}

fn post_json(body: &Value) -> Request<Body> {
    post_raw(serde_json::to_vec(body).unwrap())
}

fn post_raw(body: impl Into<Body>) -> Request<Body> {
    Request::post("/jobs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .unwrap()
}

fn get(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn submit_then_fetch(pool: PgPool) {
    let app = app(pool);

    let created = send(
        &app,
        post_json(&json!({"kind": "echo", "payload": {"hello": "world"}, "priority": 5})),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(created.body["state"], "queued");
    assert_eq!(created.body["kind"], "echo");
    assert_eq!(created.body["queue"], "default");
    assert_eq!(created.body["priority"], 5);
    assert_eq!(created.body["attempt"], 0);
    assert_eq!(created.body["payload"], json!({"hello": "world"}));
    assert_eq!(created.body["result"], Value::Null);

    let id = created.body["id"].as_str().unwrap();
    let location = created.location.unwrap();
    assert_eq!(location, format!("/jobs/{id}"));

    let fetched = send(&app, get(&location)).await;
    assert_eq!(fetched.status, StatusCode::OK);
    assert_eq!(fetched.body, created.body);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn two_identical_submissions_are_two_jobs(pool: PgPool) {
    // Without an Idempotency-Key header there is nothing to deduplicate on.
    let app = app(pool);
    let body = json!({"kind": "echo"});
    let a = send(&app, post_json(&body)).await;
    let b = send(&app, post_json(&body)).await;
    assert_eq!(
        (a.status, b.status),
        (StatusCode::CREATED, StatusCode::CREATED)
    );
    assert_ne!(a.body["id"], b.body["id"]);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn invalid_submissions_are_rejected_with_the_offending_field(pool: PgPool) {
    let app = app(pool.clone());
    let cases = [
        (json!({"kind": "Not Valid"}), "kind"),
        (json!({"kind": "shell"}), "kind"), // well-formed, but no such handler
        (json!({"kind": "echo", "queue": ""}), "queue"),
        (json!({"kind": "echo", "priority": 101}), "priority"),
        (
            json!({"kind": "echo", "delay_ms": 5, "run_at": "2030-01-01T00:00:00Z"}),
            "delay_ms",
        ),
    ];
    for (body, field) in cases {
        let reply = send(&app, post_json(&body)).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(reply.body["error"]["code"], "validation_failed", "{body}");
        assert_eq!(reply.body["error"]["field"], field, "{body}");
    }
    let unknown_kind = send(&app, post_json(&json!({"kind": "shell"}))).await;
    assert!(
        unknown_kind.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("echo, sum"),
        "the error should list the kinds that do exist"
    );

    // Nothing invalid reached the table.
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn malformed_requests_get_the_json_error_envelope(pool: PgPool) {
    let app = app(pool);

    let syntax = send(&app, post_raw("{not json")).await;
    assert_eq!(syntax.status, StatusCode::BAD_REQUEST);
    assert_eq!(syntax.body["error"]["code"], "bad_request");

    // Well-formed JSON of the wrong shape: missing `kind`, unknown field, wrong type.
    for body in [
        json!({}),
        json!({"kind": "echo", "retries": 3}),
        json!({"kind": "echo", "priority": "high"}),
        json!({"kind": "echo", "priority": 70000}),
    ] {
        let reply = send(&app, post_json(&body)).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(reply.body["error"]["code"], "validation_failed", "{body}");
    }

    let no_content_type = send(
        &app,
        Request::post("/jobs")
            .body(Body::from(r#"{"kind":"echo"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(no_content_type.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        no_content_type.body["error"]["code"],
        "unsupported_media_type"
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn oversized_bodies_are_refused(pool: PgPool) {
    let app = app(pool);
    let huge = json!({"kind": "echo", "payload": {"blob": "x".repeat(200_000)}});
    let reply = send(&app, post_json(&huge)).await;
    assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(reply.body["error"]["code"], "payload_too_large");
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn unknown_jobs_ids_and_routes(pool: PgPool) {
    let app = app(pool);

    let missing = send(&app, get("/jobs/0190f3a0-0000-7000-8000-000000000000")).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert_eq!(missing.body["error"]["code"], "not_found");

    let not_a_uuid = send(&app, get("/jobs/42")).await;
    assert_eq!(not_a_uuid.status, StatusCode::BAD_REQUEST);

    let no_route = send(&app, get("/nope")).await;
    assert_eq!(no_route.status, StatusCode::NOT_FOUND);
    assert_eq!(no_route.body["error"]["code"], "not_found");
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn health_endpoints(pool: PgPool) {
    let app = app(pool.clone());
    assert_eq!(
        send(&app, get("/healthz")).await.body,
        json!({"status": "ok"})
    );
    assert_eq!(
        send(&app, get("/readyz")).await.body,
        json!({"status": "ready"})
    );

    // Database gone: still alive, no longer ready, and writes say "come back".
    pool.close().await;
    assert_eq!(send(&app, get("/healthz")).await.status, StatusCode::OK);

    let ready = send(&app, get("/readyz")).await;
    assert_eq!(ready.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(ready.body["error"]["code"], "unavailable");

    let submit = send(&app, post_json(&json!({"kind": "echo"}))).await;
    assert_eq!(submit.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(submit.retry_after.as_deref(), Some("1"));
}
