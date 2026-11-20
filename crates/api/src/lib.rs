//! HTTP API for TaskForge.
//!
//! | Method | Path                    | Purpose                                      |
//! |--------|-------------------------|----------------------------------------------|
//! | POST   | `/jobs`                 | Submit a job (`Idempotency-Key` supported).  |
//! | GET    | `/jobs`                 | List jobs, newest first, with filters.       |
//! | GET    | `/jobs/{id}`            | Current state, result or error of a job.     |
//! | GET    | `/jobs/{id}/attempts`   | Delivery history of a job.                   |
//! | POST   | `/jobs/{id}/cancel`     | Cancel a queued job / ask a running one to stop. |
//! | POST   | `/jobs/{id}/retry`      | Re-drive a dead-lettered job.                |
//! | GET    | `/metrics`              | Prometheus metrics (OpenMetrics text).        |
//! | GET    | `/healthz`              | Liveness: the process is up.                 |
//! | GET    | `/readyz`               | Readiness: the database is reachable.        |
//!
//! Every error is JSON: `{"error": {"code", "message", "field"?}}`.

mod error;

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use taskforge_domain::{
    IdempotencyKey, Job, JobId, JobState, JobSubmission, NewJob, ValidationError,
};
use taskforge_metrics::Metrics;
use taskforge_storage::{CancelOutcome, JobStore, ListFilter, Submitted};

pub use error::ApiError;

static IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
static IDEMPOTENT_REPLAY: HeaderName = HeaderName::from_static("idempotent-replay");

/// Page size of `GET /jobs` when `limit` is not given, and its maximum.
pub const DEFAULT_PAGE_SIZE: u32 = 50;
pub const MAX_PAGE_SIZE: u32 = 200;

#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Largest request body accepted, in bytes.
    pub max_body_bytes: usize,
    /// When set, submissions for any other kind are rejected up front. The
    /// worker refuses unregistered kinds regardless; this only gives the
    /// client the bad news at submit time instead of at run time.
    pub known_kinds: Option<BTreeSet<String>>,
    /// The registry `/metrics` serves. In a process that also runs a worker
    /// or reaper, pass theirs too so one endpoint shows everything.
    pub metrics: Arc<Metrics>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            // Room for a maximum-size payload plus the envelope around it.
            max_body_bytes: taskforge_domain::MAX_PAYLOAD_BYTES + 1024,
            known_kinds: None,
            metrics: Metrics::shared(),
        }
    }
}

#[derive(Clone)]
struct AppState<S> {
    store: S,
    known_kinds: Option<Arc<BTreeSet<String>>>,
    metrics: Arc<Metrics>,
}

pub fn router<S: JobStore>(store: S, config: ApiConfig) -> Router {
    let state = AppState {
        store,
        known_kinds: config.known_kinds.map(Arc::new),
        metrics: config.metrics,
    };
    Router::new()
        .route("/jobs", post(submit_job::<S>).get(list_jobs::<S>))
        .route("/jobs/{id}", get(get_job::<S>))
        .route("/jobs/{id}/attempts", get(get_attempts::<S>))
        .route("/jobs/{id}/cancel", post(cancel_job::<S>))
        .route("/jobs/{id}/retry", post(retry_job::<S>))
        .route("/metrics", get(metrics::<S>))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz::<S>))
        .fallback(|| async { ApiError::NotFound("no such route".to_owned()) })
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .with_state(state)
}

/// `Json` extractor whose rejections are rendered as [`ApiError`], so a
/// malformed body gets the same error envelope as everything else.
#[derive(FromRequest)]
#[from_request(via(Json), rejection(ApiError))]
struct AppJson<T>(T);

/// `Query` extractor with the same treatment.
#[derive(FromRequestParts)]
#[from_request(via(axum::extract::Query), rejection(ApiError))]
struct AppQuery<T>(T);

fn parse_id(raw: &str) -> Result<JobId, ApiError> {
    raw.parse()
        .map_err(|_| ApiError::BadRequest("job id must be a UUID".to_owned()))
}

// ------------------------------------------------------------------- submit

async fn submit_job<S: JobStore>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    AppJson(submission): AppJson<JobSubmission>,
) -> Result<Response, ApiError> {
    let mut new_job = NewJob::try_from(submission)?;
    if let Some(known) = &state.known_kinds {
        if !known.contains(new_job.kind()) {
            return Err(ValidationError::new(
                "kind",
                format!(
                    "no handler is registered for {:?}; known kinds: {}",
                    new_job.kind(),
                    known
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
            .into());
        }
    }
    if let Some(raw) = headers.get(&IDEMPOTENCY_KEY) {
        // A header that is not even valid text fails the same validation.
        let key = IdempotencyKey::parse(raw.to_str().unwrap_or(""))?;
        new_job = new_job.with_idempotency_key(key);
    }

    let submitted = state.store.submit(&new_job).await?;
    state.metrics.submitted(submitted.is_duplicate());
    let job = submitted.job();
    let location = (header::LOCATION, format!("/jobs/{}", job.id));
    Ok(match &submitted {
        Submitted::Created(job) => {
            tracing::info!(job_id = %job.id, kind = %job.kind, queue = %job.queue, "job submitted");
            (StatusCode::CREATED, [location], Json(job)).into_response()
        }
        // Not an error: the client asked for this job to exist, and it does.
        // `200` (not `201`) and the header tell it nothing new was created.
        Submitted::Duplicate(job) => {
            tracing::info!(job_id = %job.id, "duplicate submission answered with the existing job");
            let mut response = (StatusCode::OK, [location], Json(job)).into_response();
            response
                .headers_mut()
                .insert(IDEMPOTENT_REPLAY.clone(), HeaderValue::from_static("true"));
            response
        }
    })
}

// -------------------------------------------------------------------- reads

async fn get_job<S: JobStore>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<Json<Job>, ApiError> {
    let id = parse_id(&id)?;
    let job = state
        .store
        .get(id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("job {id} not found")))?;
    Ok(Json(job))
}

async fn get_attempts<S: JobStore>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let id = parse_id(&id)?;
    if state.store.get(id).await?.is_none() {
        return Err(ApiError::NotFound(format!("job {id} not found")));
    }
    let attempts = state.store.attempts(id).await?;
    Ok(Json(json!({ "attempts": attempts })))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListParams {
    state: Option<String>,
    queue: Option<String>,
    kind: Option<String>,
    /// Cursor: the `next_before` value of the previous page.
    before: Option<String>,
    limit: Option<u32>,
}

async fn list_jobs<S: JobStore>(
    State(state): State<AppState<S>>,
    AppQuery(params): AppQuery<ListParams>,
) -> Result<impl IntoResponse, ApiError> {
    let job_state = params
        .state
        .map(|raw| {
            raw.parse::<JobState>().map_err(|_| {
                ValidationError::new(
                    "state",
                    "must be one of queued, running, succeeded, dead, cancelled",
                )
            })
        })
        .transpose()?;
    let before = params.before.as_deref().map(parse_id).transpose()?;
    let limit = params.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ValidationError::new(
            "limit",
            format!("must be between 1 and {MAX_PAGE_SIZE}"),
        )
        .into());
    }

    let jobs = state
        .store
        .list(&ListFilter {
            state: job_state,
            queue: params.queue,
            kind: params.kind,
            before,
            limit,
        })
        .await?;
    // A full page may have more behind it; hand out the cursor to find out.
    let next_before = (jobs.len() as u32 == limit)
        .then(|| jobs.last().map(|job| job.id))
        .flatten();
    Ok(Json(json!({ "jobs": jobs, "next_before": next_before })))
}

// ------------------------------------------------------------------ actions

async fn cancel_job<S: JobStore>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = parse_id(&id)?;
    Ok(match state.store.request_cancel(id).await? {
        CancelOutcome::Cancelled(job) => {
            tracing::info!(job_id = %job.id, "job cancelled");
            (StatusCode::OK, Json(job)).into_response()
        }
        // Accepted, not done: the job is still running and may yet succeed.
        CancelOutcome::Requested(job) => {
            tracing::info!(job_id = %job.id, "cancellation requested for a running job");
            (StatusCode::ACCEPTED, Json(job)).into_response()
        }
        CancelOutcome::AlreadyFinished(job) => ApiError::Conflict {
            code: "already_finished",
            message: format!("job is already {}; nothing to cancel", job.state),
        }
        .into_response(),
    })
}

async fn retry_job<S: JobStore>(
    State(state): State<AppState<S>>,
    Path(id): Path<String>,
) -> Result<Json<Job>, ApiError> {
    let id = parse_id(&id)?;
    let job = state.store.redrive(id).await?;
    tracing::info!(job_id = %job.id, "dead-lettered job re-driven");
    Ok(Json(job))
}

// ------------------------------------------------------------------- health

/// Process counters plus queue-wide gauges read from the store at scrape
/// time. If the store cannot be read, the counters are still served (they are
/// exactly what you want during an outage) with the gauges left as they were.
async fn metrics<S: JobStore>(State(state): State<AppState<S>>) -> Response {
    match state.store.stats().await {
        Ok(stats) => state.metrics.observe_stats(&stats),
        Err(err) => tracing::warn!(error = %err, "metrics: could not read queue statistics"),
    }
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        state.metrics.encode(),
    )
        .into_response()
}

async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn readyz<S: JobStore>(
    State(state): State<AppState<S>>,
) -> Result<impl IntoResponse, ApiError> {
    state.store.ping().await?;
    Ok(Json(json!({ "status": "ready" })))
}
