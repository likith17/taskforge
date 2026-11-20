use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::JobState;

pub const DEFAULT_QUEUE: &str = "default";
/// Maximum length of a job kind or queue name.
pub const MAX_NAME_LEN: usize = 64;
pub const MIN_PRIORITY: i16 = -100;
pub const MAX_PRIORITY: i16 = 100;
/// Maximum size of a job payload once serialized to JSON.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;
/// Upper bound on `max_attempts`. Also the length of the backoff table the
/// storage layer passes to PostgreSQL.
pub const MAX_MAX_ATTEMPTS: u32 = 25;
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;
/// Furthest into the future a job may be delayed.
pub const MAX_DELAY: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// Identifies a job. UUIDv7, so ids sort by creation time and index well.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for JobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// A job as persisted.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Job {
    pub id: JobId,
    pub queue: String,
    pub kind: String,
    pub payload: Value,
    pub state: JobState,
    pub priority: i16,
    /// Not claimable before this instant.
    pub run_at: DateTime<Utc>,
    /// Number of times the job has been claimed. Doubles as the fencing token:
    /// only the holder of the current attempt may settle the job.
    pub attempt: i32,
    /// Attempts that ended badly (error, panic, expired lease). The job is
    /// dead-lettered when this reaches `max_attempts`.
    pub failures: i32,
    pub max_attempts: i32,
    pub idempotency_key: Option<String>,
    /// Someone asked for this job to be cancelled. For a running job this is a
    /// request the worker has not acted on yet.
    pub cancel_requested: bool,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub result: Option<Value>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// When the most recent attempt started.
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// When a new job becomes claimable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Schedule {
    Immediately,
    /// At an absolute instant (a past instant means "now").
    At(DateTime<Utc>),
    /// This long after the job is inserted, measured on the database clock.
    After(Duration),
}

/// The wire form of a submission, exactly as a client sends it. Unvalidated.
///
/// Unknown fields are rejected rather than ignored, so a client relying on an
/// option this version does not implement finds out immediately.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSubmission {
    pub kind: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub queue: Option<String>,
    #[serde(default)]
    pub priority: Option<i16>,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub delay_ms: Option<u64>,
    /// How many failed attempts are allowed before the job is dead-lettered.
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

/// A client-chosen key that makes a submission safe to repeat: every request
/// carrying the same key refers to the same job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// 1 to 255 printable ASCII characters, no spaces.
    pub fn parse(raw: &str) -> Result<Self, ValidationError> {
        let ok = !raw.is_empty()
            && raw.len() <= MAX_IDEMPOTENCY_KEY_LEN
            && raw.bytes().all(|b| b.is_ascii_graphic());
        if !ok {
            return Err(ValidationError::new(
                "Idempotency-Key",
                format!(
                    "must be 1-{MAX_IDEMPOTENCY_KEY_LEN} printable ASCII characters without spaces"
                ),
            ));
        }
        Ok(Self(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{field}: {message}")]
pub struct ValidationError {
    pub field: &'static str,
    pub message: String,
}

impl ValidationError {
    pub fn new(field: &'static str, message: impl Into<String>) -> Self {
        Self {
            field,
            message: message.into(),
        }
    }
}

/// A submission that has passed validation. The only way to obtain one is
/// [`NewJob::try_from`], so storage never sees unchecked input.
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    id: JobId,
    queue: String,
    kind: String,
    payload: Value,
    priority: i16,
    schedule: Schedule,
    max_attempts: i32,
    idempotency_key: Option<IdempotencyKey>,
}

impl NewJob {
    /// Attach an idempotency key (from the `Idempotency-Key` header).
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Whether `existing` is what this submission would have created. Used
    /// when an idempotency key is seen again: the same key with a different
    /// request is a client bug and must not be answered with someone else's
    /// job.
    ///
    /// The schedule is deliberately not compared: a relative `delay_ms`
    /// resolves to a different instant on every retry of the same request.
    pub fn same_request_as(&self, existing: &Job) -> bool {
        self.kind == existing.kind
            && self.queue == existing.queue
            && self.payload == existing.payload
            && self.priority == existing.priority
            && self.max_attempts == existing.max_attempts
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn priority(&self) -> i16 {
        self.priority
    }

    pub fn schedule(&self) -> Schedule {
        self.schedule
    }
}

impl TryFrom<JobSubmission> for NewJob {
    type Error = ValidationError;

    fn try_from(s: JobSubmission) -> Result<Self, Self::Error> {
        validate_name("kind", &s.kind)?;
        let queue = s.queue.unwrap_or_else(|| DEFAULT_QUEUE.to_owned());
        validate_name("queue", &queue)?;

        let priority = s.priority.unwrap_or(0);
        if !(MIN_PRIORITY..=MAX_PRIORITY).contains(&priority) {
            return Err(ValidationError::new(
                "priority",
                format!("must be between {MIN_PRIORITY} and {MAX_PRIORITY}"),
            ));
        }

        let schedule = match (s.run_at, s.delay_ms) {
            (Some(_), Some(_)) => {
                return Err(ValidationError::new(
                    "delay_ms",
                    "give either run_at or delay_ms, not both",
                ));
            }
            (Some(at), None) => {
                if at > Utc::now() + MAX_DELAY {
                    return Err(ValidationError::new(
                        "run_at",
                        "must be less than 365 days in the future",
                    ));
                }
                Schedule::At(at)
            }
            (None, Some(ms)) => {
                let delay = Duration::from_millis(ms);
                if delay > MAX_DELAY {
                    return Err(ValidationError::new("delay_ms", "must be at most 365 days"));
                }
                Schedule::After(delay)
            }
            (None, None) => Schedule::Immediately,
        };

        let payload = match s.payload {
            None | Some(Value::Null) => Value::Object(Default::default()),
            Some(value) => value,
        };
        // `Value` always serializes; fall back to "too large" defensively.
        let size = serde_json::to_vec(&payload).map_or(usize::MAX, |bytes| bytes.len());
        if size > MAX_PAYLOAD_BYTES {
            return Err(ValidationError::new(
                "payload",
                format!("must be at most {MAX_PAYLOAD_BYTES} bytes of JSON, got {size}"),
            ));
        }

        let max_attempts = s.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if !(1..=MAX_MAX_ATTEMPTS).contains(&max_attempts) {
            return Err(ValidationError::new(
                "max_attempts",
                format!("must be between 1 and {MAX_MAX_ATTEMPTS}"),
            ));
        }

        Ok(NewJob {
            id: JobId::new(),
            queue,
            kind: s.kind,
            payload,
            priority,
            schedule,
            max_attempts: max_attempts as i32, // range-checked above
            idempotency_key: None,
        })
    }
}

/// Kinds and queue names: a lowercase letter, then lowercase letters, digits,
/// `_`, `-` or `.`. A deliberately narrow alphabet: these strings end up in
/// logs, metrics labels and URLs.
fn validate_name(field: &'static str, value: &str) -> Result<(), ValidationError> {
    let mut chars = value.chars();
    let starts_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    let rest_ok =
        chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '-' | '.'));
    if !starts_ok || !rest_ok || value.len() > MAX_NAME_LEN {
        return Err(ValidationError::new(
            field,
            format!(
                "must be 1-{MAX_NAME_LEN} characters: a lowercase letter followed by lowercase letters, digits, '_', '-' or '.'"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn submission(kind: &str) -> JobSubmission {
        JobSubmission {
            kind: kind.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn minimal_submission_gets_defaults() {
        let job = NewJob::try_from(submission("echo")).unwrap();
        assert_eq!(job.kind(), "echo");
        assert_eq!(job.queue(), DEFAULT_QUEUE);
        assert_eq!(job.priority(), 0);
        assert_eq!(job.schedule(), Schedule::Immediately);
        assert_eq!(job.payload(), &json!({}));
    }

    #[test]
    fn each_new_job_gets_a_distinct_time_ordered_id() {
        let a = NewJob::try_from(submission("echo")).unwrap().id();
        let b = NewJob::try_from(submission("echo")).unwrap().id();
        assert_ne!(a, b);
        assert_eq!(a.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn rejects_bad_kind_and_queue_names() {
        for bad in [
            "",
            "Echo",
            "1echo",
            "echo job",
            "echo;drop",
            "é",
            &"a".repeat(65),
        ] {
            let err = NewJob::try_from(submission(bad)).unwrap_err();
            assert_eq!(err.field, "kind", "{bad:?}");

            let mut s = submission("echo");
            s.queue = Some(bad.to_owned());
            assert_eq!(NewJob::try_from(s).unwrap_err().field, "queue", "{bad:?}");
        }
        assert!(NewJob::try_from(submission(&"a".repeat(64))).is_ok());
        assert!(NewJob::try_from(submission("email.send-v2_x")).is_ok());
    }

    #[test]
    fn rejects_out_of_range_priority() {
        for (priority, ok) in [(-101, false), (-100, true), (100, true), (101, false)] {
            let mut s = submission("echo");
            s.priority = Some(priority);
            assert_eq!(NewJob::try_from(s).is_ok(), ok, "priority {priority}");
        }
    }

    #[test]
    fn schedule_is_either_absolute_or_relative() {
        let at = Utc::now() + chrono::Duration::minutes(5);

        let mut s = submission("echo");
        s.run_at = Some(at);
        assert_eq!(NewJob::try_from(s).unwrap().schedule(), Schedule::At(at));

        let mut s = submission("echo");
        s.delay_ms = Some(1500);
        assert_eq!(
            NewJob::try_from(s).unwrap().schedule(),
            Schedule::After(Duration::from_millis(1500))
        );

        let mut s = submission("echo");
        s.run_at = Some(at);
        s.delay_ms = Some(1);
        assert_eq!(NewJob::try_from(s).unwrap_err().field, "delay_ms");
    }

    #[test]
    fn rejects_schedules_more_than_a_year_out() {
        let mut s = submission("echo");
        s.delay_ms = Some(MAX_DELAY.as_millis() as u64 + 1);
        assert_eq!(NewJob::try_from(s).unwrap_err().field, "delay_ms");

        let mut s = submission("echo");
        s.run_at = Some(Utc::now() + chrono::Duration::days(366));
        assert_eq!(NewJob::try_from(s).unwrap_err().field, "run_at");
    }

    #[test]
    fn rejects_oversized_payload() {
        let mut s = submission("echo");
        s.payload = Some(json!({ "blob": "x".repeat(MAX_PAYLOAD_BYTES) }));
        assert_eq!(NewJob::try_from(s).unwrap_err().field, "payload");
    }

    #[test]
    fn unknown_fields_are_rejected_at_deserialization() {
        let err = serde_json::from_value::<JobSubmission>(json!({
            "kind": "echo",
            "retries": 3
        }))
        .unwrap_err();
        assert!(err.to_string().contains("retries"));
    }

    #[test]
    fn max_attempts_defaults_and_is_bounded() {
        let job = NewJob::try_from(submission("echo")).unwrap();
        assert_eq!(job.max_attempts(), DEFAULT_MAX_ATTEMPTS as i32);

        for (value, ok) in [(0, false), (1, true), (25, true), (26, false)] {
            let mut s = submission("echo");
            s.max_attempts = Some(value);
            let result = NewJob::try_from(s);
            assert_eq!(result.is_ok(), ok, "max_attempts {value}");
            if let Err(err) = result {
                assert_eq!(err.field, "max_attempts");
            }
        }
    }

    #[test]
    fn idempotency_keys_are_printable_ascii_without_spaces() {
        for good in ["a", "order-8841", "01HZX:retry/3", &"k".repeat(255)] {
            assert_eq!(IdempotencyKey::parse(good).unwrap().as_str(), good);
        }
        for bad in ["", " ", "has space", "tab\there", "naïve", &"k".repeat(256)] {
            let err = IdempotencyKey::parse(bad).unwrap_err();
            assert_eq!(err.field, "Idempotency-Key", "{bad:?}");
        }
    }

    #[test]
    fn idempotency_key_is_attached_separately_from_the_body() {
        let job = NewJob::try_from(submission("echo")).unwrap();
        assert!(job.idempotency_key().is_none());
        let keyed = job.with_idempotency_key(IdempotencyKey::parse("k1").unwrap());
        assert_eq!(keyed.idempotency_key().unwrap().as_str(), "k1");
    }
}
