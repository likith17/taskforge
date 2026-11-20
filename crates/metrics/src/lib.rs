//! Prometheus metrics for TaskForge.
//!
//! Each process keeps its own counters and exposes them on `/metrics`; a
//! Prometheus server scrapes every process and sums across them. Counters
//! therefore describe what *this process* did. Queue-wide numbers (jobs per
//! state, age of the oldest due job, expired leases nobody has reaped) come
//! from the database at scrape time, via [`Metrics::observe_stats`].
//!
//! Dependency: `prometheus-client`, the Prometheus project's own Rust client.
//! It implements the exposition format and histogram bookkeeping, and has no
//! HTTP server of its own; the API serves its output through Axum.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use taskforge_storage::{QueueStats, Reaped};

type Labels = Vec<(&'static str, String)>;

/// How a worker's attempt at a job ended, as a metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    Succeeded,
    Retrying,
    DeadLettered,
    Cancelled,
    Released,
    /// The lease was lost; the result was discarded.
    LostClaim,
    /// The outcome could not be written; lease expiry will recover the job.
    Unreported,
}

impl JobOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            JobOutcome::Succeeded => "succeeded",
            JobOutcome::Retrying => "retrying",
            JobOutcome::DeadLettered => "dead_lettered",
            JobOutcome::Cancelled => "cancelled",
            JobOutcome::Released => "released",
            JobOutcome::LostClaim => "lost_claim",
            JobOutcome::Unreported => "unreported",
        }
    }
}

#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    job_outcomes: Family<Labels, Counter>,
    lease_expirations: Family<Labels, Counter>,
    submissions: Family<Labels, Counter>,
    jobs_purged: Counter,
    heartbeat_failures: Counter,
    claim_duration: Histogram,
    queue_wait: Histogram,
    execution_duration: Histogram,
    jobs: Family<Labels, Gauge>,
    oldest_due: Gauge<f64, AtomicU64>,
    expired_leases: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("taskforge");

        let job_outcomes = Family::<Labels, Counter>::default();
        registry.register(
            "worker_job_outcomes",
            "Attempts finished by this worker, by outcome",
            job_outcomes.clone(),
        );
        let lease_expirations = Family::<Labels, Counter>::default();
        registry.register(
            "lease_expirations",
            "Leases this process found expired and took back, by what happened to the job",
            lease_expirations.clone(),
        );
        let submissions = Family::<Labels, Counter>::default();
        registry.register(
            "submissions",
            "Jobs submitted through this API process (created, or duplicate of an idempotency key)",
            submissions.clone(),
        );
        let jobs_purged = Counter::default();
        registry.register(
            "jobs_purged",
            "Finished jobs deleted after the retention period",
            jobs_purged.clone(),
        );
        let heartbeat_failures = Counter::default();
        registry.register(
            "heartbeat_failures",
            "Lease renewals that could not reach the database",
            heartbeat_failures.clone(),
        );
        // 0.5 ms .. ~16 s
        let claim_duration = Histogram::new(exponential_buckets(0.0005, 2.0, 16));
        registry.register(
            "claim_duration_seconds",
            "Round trip of one claim query, including empty polls",
            claim_duration.clone(),
        );
        // 1 ms .. ~9 min
        let queue_wait = Histogram::new(exponential_buckets(0.001, 2.0, 20));
        registry.register(
            "queue_wait_seconds",
            "Time a job was due before a worker claimed it (database clock)",
            queue_wait.clone(),
        );
        let execution_duration = Histogram::new(exponential_buckets(0.001, 2.0, 20));
        registry.register(
            "job_execution_seconds",
            "Handler run time per attempt",
            execution_duration.clone(),
        );
        let jobs = Family::<Labels, Gauge>::default();
        registry.register(
            "jobs",
            "Jobs in the database, by queue and state",
            jobs.clone(),
        );
        let oldest_due = Gauge::<f64, AtomicU64>::default();
        registry.register(
            "oldest_due_job_age_seconds",
            "How long the oldest due, unclaimed job has been waiting",
            oldest_due.clone(),
        );
        let expired_leases = Gauge::default();
        registry.register(
            "expired_unreaped_leases",
            "Running jobs whose lease has expired but which no reaper has taken back; \
             persistently above zero means no reaper is running",
            expired_leases.clone(),
        );

        Self {
            registry,
            job_outcomes,
            lease_expirations,
            submissions,
            jobs_purged,
            heartbeat_failures,
            claim_duration,
            queue_wait,
            execution_duration,
            jobs,
            oldest_due,
            expired_leases,
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn job_outcome(&self, outcome: JobOutcome) {
        self.job_outcomes
            .get_or_create(&vec![("outcome", outcome.as_str().to_owned())])
            .inc();
    }

    pub fn reaped(&self, reaped: Reaped) {
        for (label, n) in [
            ("requeued", reaped.requeued),
            ("dead_lettered", reaped.dead),
            ("cancelled", reaped.cancelled),
        ] {
            if n > 0 {
                self.lease_expirations
                    .get_or_create(&vec![("result", label.to_owned())])
                    .inc_by(n);
            }
        }
    }

    pub fn submitted(&self, duplicate: bool) {
        let result = if duplicate { "duplicate" } else { "created" };
        self.submissions
            .get_or_create(&vec![("result", result.to_owned())])
            .inc();
    }

    pub fn purged(&self, n: u64) {
        self.jobs_purged.inc_by(n);
    }

    pub fn heartbeat_failed(&self) {
        self.heartbeat_failures.inc();
    }

    pub fn claim_took(&self, seconds: f64) {
        self.claim_duration.observe(seconds);
    }

    pub fn queue_waited(&self, seconds: f64) {
        self.queue_wait.observe(seconds.max(0.0));
    }

    pub fn executed_for(&self, seconds: f64) {
        self.execution_duration.observe(seconds);
    }

    /// Refresh the database-derived gauges.
    pub fn observe_stats(&self, stats: &QueueStats) {
        // Start from empty so that a state with no jobs left disappears
        // instead of reporting its last non-zero value forever.
        self.jobs.clear();
        for (queue, state, n) in &stats.counts {
            self.jobs
                .get_or_create(&vec![
                    ("queue", queue.clone()),
                    ("state", state.as_str().to_owned()),
                ])
                .set(*n);
        }
        self.oldest_due.set(stats.oldest_due_secs);
        self.expired_leases.set(stats.expired_leases);
    }

    /// The Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let mut out = String::new();
        // Writing into a String cannot fail.
        let _ = encode(&mut out, &self.registry);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition_contains_every_metric_with_its_labels() {
        let m = Metrics::new();
        m.job_outcome(JobOutcome::Succeeded);
        m.job_outcome(JobOutcome::Succeeded);
        m.job_outcome(JobOutcome::DeadLettered);
        m.reaped(Reaped {
            requeued: 3,
            dead: 1,
            cancelled: 0,
        });
        m.submitted(false);
        m.submitted(true);
        m.claim_took(0.002);
        m.queue_waited(0.5);
        m.executed_for(0.01);
        m.purged(4);
        m.heartbeat_failed();

        let text = m.encode();
        for expected in [
            r#"taskforge_worker_job_outcomes_total{outcome="succeeded"} 2"#,
            r#"taskforge_worker_job_outcomes_total{outcome="dead_lettered"} 1"#,
            r#"taskforge_lease_expirations_total{result="requeued"} 3"#,
            r#"taskforge_lease_expirations_total{result="dead_lettered"} 1"#,
            r#"taskforge_submissions_total{result="duplicate"} 1"#,
            "taskforge_jobs_purged_total 4",
            "taskforge_heartbeat_failures_total 1",
            "taskforge_claim_duration_seconds_count 1",
            "taskforge_queue_wait_seconds_sum 0.5",
            "taskforge_job_execution_seconds_count 1",
        ] {
            assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
        }
        assert!(
            !text.contains(r#"result="cancelled""#),
            "zero counts are not created"
        );
    }

    #[test]
    fn stats_gauges_are_replaced_not_accumulated() {
        use taskforge_storage::QueueStats;

        let m = Metrics::new();
        let state = |s: &str| s.parse().unwrap();
        m.observe_stats(&QueueStats {
            counts: vec![
                ("default".into(), state("queued"), 7),
                ("default".into(), state("running"), 2),
            ],
            oldest_due_secs: 1.5,
            expired_leases: 1,
        });
        m.observe_stats(&QueueStats {
            counts: vec![("default".into(), state("succeeded"), 9)],
            oldest_due_secs: 0.0,
            expired_leases: 0,
        });
        let text = m.encode();
        assert!(
            text.contains(r#"taskforge_jobs{queue="default",state="succeeded"} 9"#),
            "{text}"
        );
        assert!(
            !text.contains(r#"state="queued""#),
            "stale label left behind:\n{text}"
        );
        assert!(
            text.contains("taskforge_oldest_due_job_age_seconds 0.0"),
            "{text}"
        );
        assert!(
            text.contains("taskforge_expired_unreaped_leases 0"),
            "{text}"
        );
    }
}
