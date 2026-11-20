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
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            // Room for a maximum-size payload plus the envelope around it.
            max_body_bytes: taskforge_domain::MAX_PAYLOAD_BYTES + 1024,
            known_kinds: None,
        }
    }
}

#[derive(Clone)]
struct AppState {
    store: JobStore,
    known_kinds: Option<Arc<BTreeSet<String>>>,
}

pub fn router(store: JobStore, config: ApiConfig) -> Router {
    let state = AppState {
        store,
        known_kinds: config.known_kinds.map(Arc::new),
    };
    Router::new()
        .route("/jobs", post(submit_job).get(list_jobs))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/attempts", get(get_attempts))
        .route("/jobs/{id}/cancel", post(cancel_job))
        .route("/jobs/{id}/retry", post(retry_job))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
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

async fn submit_job(
    State(state): State<AppState>,
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

async fn get_job(
    State(state): State<AppState>,
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

async fn get_attempts(
    State(state): State<AppState>,
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

async fn list_jobs(
    State(state): State<AppState>,
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

async fn cancel_job(
    State(state): State<AppState>,
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

async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Job>, ApiError> {
    let id = parse_id(&id)?;
    let job = state.store.redrive(id).await?;
    tracing::info!(job_id = %job.id, "dead-lettered job re-driven");
    Ok(Json(job))
}

// ------------------------------------------------------------------- health

async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    state.store.ping().await?;
    Ok(Json(json!({ "status": "ready" })))
}
