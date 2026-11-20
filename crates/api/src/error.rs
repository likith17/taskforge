use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;
use taskforge_domain::ValidationError;
use taskforge_storage::StoreError;

#[derive(Debug)]
pub enum ApiError {
    /// The request was understood but its content is not acceptable.
    Validation(ValidationError),
    BadRequest(String),
    NotFound(String),
    /// The request is valid but conflicts with the current state of things.
    Conflict {
        code: &'static str,
        message: String,
    },
    /// The body could not be read as the expected JSON.
    Rejected {
        status: StatusCode,
        message: String,
    },
    /// The database is unreachable; the same request may succeed later.
    Unavailable,
    Internal,
}

impl From<ValidationError> for ApiError {
    fn from(err: ValidationError) -> Self {
        ApiError::Validation(err)
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        ApiError::Rejected {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

impl From<QueryRejection> for ApiError {
    fn from(rejection: QueryRejection) -> Self {
        ApiError::BadRequest(rejection.body_text())
    }
}

impl From<StoreError> for ApiError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::NotFound(id) => ApiError::NotFound(format!("job {id} not found")),
            StoreError::IdempotencyConflict { .. } => ApiError::Conflict {
                code: "idempotency_key_reused",
                message: "this Idempotency-Key was already used for a different request".to_owned(),
            },
            StoreError::InvalidState { state, action, .. } => ApiError::Conflict {
                code: "invalid_state",
                message: format!("cannot {action} a job that is {state}"),
            },
            // Log the detail; never send database internals to the client.
            err if err.is_unavailable() => {
                tracing::warn!(error = %err, "database unavailable");
                ApiError::Unavailable
            }
            err => {
                tracing::error!(error = %err, "storage error");
                ApiError::Internal
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, field) = match self {
            ApiError::Validation(err) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_failed",
                err.message,
                Some(err.field),
            ),
            ApiError::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, "bad_request", message, None)
            }
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message, None),
            ApiError::Conflict { code, message } => (StatusCode::CONFLICT, code, message, None),
            ApiError::Rejected { status, message } => {
                let code = match status {
                    StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
                    StatusCode::UNSUPPORTED_MEDIA_TYPE => "unsupported_media_type",
                    StatusCode::UNPROCESSABLE_ENTITY => "validation_failed",
                    _ => "bad_request",
                };
                (status, code, message, None)
            }
            ApiError::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "the database is temporarily unavailable; retry shortly".to_owned(),
                None,
            ),
            ApiError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "internal error".to_owned(),
                None,
            ),
        };

        let mut error = json!({ "code": code, "message": message });
        if let Some(field) = field {
            error["field"] = json!(field);
        }
        let mut response = (status, Json(json!({ "error": error }))).into_response();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
        }
        response
    }
}
