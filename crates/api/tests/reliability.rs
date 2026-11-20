//! Milestone 2 endpoints: idempotent submission, listing, cancel, retry,
//! attempt history.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use taskforge_api::{ApiConfig, router};
use taskforge_storage::JobStore;
use tower::ServiceExt;

const LEASE: Duration = Duration::from_secs(60);

fn queues() -> Vec<String> {
    vec!["default".to_owned()]
}

struct Reply {
    status: StatusCode,
    replay: Option<String>,
    body: Value,
}

async fn send(app: &Router, request: Request<Body>) -> Reply {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let replay = response
        .headers()
        .get("idempotent-replay")
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("non-JSON body: {:?}", String::from_utf8_lossy(&bytes)));
    Reply {
        status,
        replay,
        body,
    }
}

fn submit(body: &Value, key: Option<&str>) -> Request<Body> {
    let mut request = Request::post("/jobs").header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    request
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

fn call(method: Method, path: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .unwrap()
}

fn get(path: &str) -> Request<Body> {
    call(Method::GET, path)
}

fn post(path: &str) -> Request<Body> {
    call(Method::POST, path)
}

async fn new_job(app: &Router, body: Value) -> String {
    let reply = send(app, submit(&body, None)).await;
    assert_eq!(reply.status, StatusCode::CREATED);
    reply.body["id"].as_str().unwrap().to_owned()
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn idempotency_key_makes_a_retried_submission_safe(pool: PgPool) {
    let app = router(JobStore::new(pool.clone()), ApiConfig::default());
    let body = json!({"kind": "echo", "payload": {"charge": 500}});

    let first = send(&app, submit(&body, Some("order-8841"))).await;
    assert_eq!(first.status, StatusCode::CREATED);
    assert_eq!(first.replay, None);
    assert_eq!(first.body["idempotency_key"], "order-8841");

    // The client never saw that response and sends the request again.
    let retry = send(&app, submit(&body, Some("order-8841"))).await;
    assert_eq!(retry.status, StatusCode::OK);
    assert_eq!(retry.replay.as_deref(), Some("true"));
    assert_eq!(retry.body["id"], first.body["id"]);

    // The same key for something else is a client bug, not a duplicate.
    let other = json!({"kind": "echo", "payload": {"charge": 999}});
    let conflict = send(&app, submit(&other, Some("order-8841"))).await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.body["error"]["code"], "idempotency_key_reused");

    // A malformed key is refused before anything is stored.
    let bad = send(&app, submit(&body, Some("has space"))).await;
    assert_eq!(bad.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(bad.body["error"]["field"], "Idempotency-Key");

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn concurrent_duplicate_requests_create_one_job(pool: PgPool) {
    let app = router(JobStore::new(pool.clone()), ApiConfig::default());
    let body = json!({"kind": "echo"});

    let mut requests = Vec::new();
    for _ in 0..25 {
        let app = app.clone();
        let body = body.clone();
        requests.push(tokio::spawn(async move {
            let reply = send(&app, submit(&body, Some("storm"))).await;
            (reply.status, reply.body["id"].clone())
        }));
    }
    let mut created = 0;
    let mut ids = std::collections::HashSet::new();
    for request in requests {
        let (status, id) = request.await.unwrap();
        assert!(
            status == StatusCode::CREATED || status == StatusCode::OK,
            "{status}"
        );
        created += usize::from(status == StatusCode::CREATED);
        ids.insert(id.to_string());
    }
    assert_eq!(created, 1);
    assert_eq!(ids.len(), 1);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn max_attempts_is_accepted_and_validated(pool: PgPool) {
    let app = router(JobStore::new(pool), ApiConfig::default());

    let created = send(
        &app,
        submit(&json!({"kind": "echo", "max_attempts": 8}), None),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(created.body["max_attempts"], 8);
    assert_eq!(created.body["failures"], 0);

    let default = send(&app, submit(&json!({"kind": "echo"}), None)).await;
    assert_eq!(default.body["max_attempts"], 5);

    let invalid = send(
        &app,
        submit(&json!({"kind": "echo", "max_attempts": 0}), None),
    )
    .await;
    assert_eq!(invalid.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid.body["error"]["field"], "max_attempts");
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn list_filters_validates_and_paginates(pool: PgPool) {
    let app = router(JobStore::new(pool), ApiConfig::default());
    let mut ids = Vec::new();
    for i in 0..5 {
        let kind = if i % 2 == 0 { "echo" } else { "sum" };
        ids.push(new_job(&app, json!({"kind": kind, "queue": "reports"})).await);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    ids.reverse(); // newest first

    let all = send(&app, get("/jobs")).await;
    assert_eq!(all.status, StatusCode::OK);
    let listed: Vec<&str> = all.body["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j["id"].as_str().unwrap())
        .collect();
    assert_eq!(listed, ids);
    assert_eq!(
        all.body["next_before"],
        Value::Null,
        "a short page is the last page"
    );

    // Two per page, following the cursor.
    let page1 = send(&app, get("/jobs?limit=2")).await;
    assert_eq!(page1.body["jobs"].as_array().unwrap().len(), 2);
    let cursor = page1.body["next_before"].as_str().unwrap();
    assert_eq!(cursor, ids[1]);
    let page2 = send(&app, get(&format!("/jobs?limit=2&before={cursor}"))).await;
    assert_eq!(page2.body["jobs"][0]["id"], ids[2]);

    let sums = send(&app, get("/jobs?kind=sum&queue=reports&state=queued")).await;
    assert_eq!(sums.body["jobs"].as_array().unwrap().len(), 2);
    let none = send(&app, get("/jobs?state=dead")).await;
    assert_eq!(none.body["jobs"], json!([]));

    for (query, field) in [
        ("state=paused", "state"),
        ("limit=0", "limit"),
        ("limit=201", "limit"),
    ] {
        let reply = send(&app, get(&format!("/jobs?{query}"))).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY, "{query}");
        assert_eq!(reply.body["error"]["field"], field, "{query}");
    }
    for query in ["limit=lots", "before=not-a-uuid", "colour=blue"] {
        let reply = send(&app, get(&format!("/jobs?{query}"))).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(reply.body["error"]["code"], "bad_request", "{query}");
    }
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn cancel_reports_what_actually_happened(pool: PgPool) {
    let store = JobStore::new(pool);
    let app = router(store.clone(), ApiConfig::default());

    // Queued: cancelled now. Repeating the request is harmless.
    let queued = new_job(&app, json!({"kind": "echo", "delay_ms": 60000})).await;
    for _ in 0..2 {
        let reply = send(&app, post(&format!("/jobs/{queued}/cancel"))).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.body["state"], "cancelled");
    }

    // Running: only a request, so 202 and still running.
    let running = new_job(&app, json!({"kind": "echo"})).await;
    let claim = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap()
        .claim;
    let reply = send(&app, post(&format!("/jobs/{running}/cancel"))).await;
    assert_eq!(reply.status, StatusCode::ACCEPTED);
    assert_eq!(reply.body["state"], "running");
    assert_eq!(reply.body["cancel_requested"], true);

    // The worker finishes first: the job succeeded, and cancel now says so.
    store.complete(&claim, &json!("done")).await.unwrap();
    let late = send(&app, post(&format!("/jobs/{running}/cancel"))).await;
    assert_eq!(late.status, StatusCode::CONFLICT);
    assert_eq!(late.body["error"]["code"], "already_finished");
    assert_eq!(
        send(&app, get(&format!("/jobs/{running}"))).await.body["state"],
        "succeeded"
    );

    let missing = send(
        &app,
        post("/jobs/0190f3a0-0000-7000-8000-000000000000/cancel"),
    )
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert_eq!(
        send(&app, post("/jobs/42/cancel")).await.status,
        StatusCode::BAD_REQUEST
    );
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn retry_redrives_only_dead_jobs_and_attempts_show_the_history(pool: PgPool) {
    let store = JobStore::new(pool);
    let app = router(store.clone(), ApiConfig::default());
    let id = new_job(&app, json!({"kind": "echo", "max_attempts": 1})).await;

    // Not dead yet: nothing to re-drive.
    let early = send(&app, post(&format!("/jobs/{id}/retry"))).await;
    assert_eq!(early.status, StatusCode::CONFLICT);
    assert_eq!(early.body["error"]["code"], "invalid_state");

    let claim = store
        .claim_next("w1", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap()
        .claim;
    store.fail(&claim, "upstream said no", true).await.unwrap();
    assert_eq!(
        send(&app, get(&format!("/jobs/{id}"))).await.body["state"],
        "dead"
    );

    let revived = send(&app, post(&format!("/jobs/{id}/retry"))).await;
    assert_eq!(revived.status, StatusCode::OK);
    assert_eq!(revived.body["state"], "queued");
    assert_eq!(revived.body["failures"], 0);

    let claim = store
        .claim_next("w2", &queues(), LEASE)
        .await
        .unwrap()
        .unwrap()
        .claim;
    store.complete(&claim, &json!("ok")).await.unwrap();

    let history = send(&app, get(&format!("/jobs/{id}/attempts"))).await;
    assert_eq!(history.status, StatusCode::OK);
    let attempts = history.body["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["attempt"], 1);
    assert_eq!(attempts[0]["worker_id"], "w1");
    assert_eq!(attempts[0]["outcome"], "failed");
    assert_eq!(attempts[0]["error"], "upstream said no");
    assert_eq!(attempts[1]["worker_id"], "w2");
    assert_eq!(attempts[1]["outcome"], "succeeded");
    assert_eq!(attempts[1]["error"], Value::Null);

    let missing = send(
        &app,
        get("/jobs/0190f3a0-0000-7000-8000-000000000000/attempts"),
    )
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    let missing = send(
        &app,
        post("/jobs/0190f3a0-0000-7000-8000-000000000000/retry"),
    )
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
}
