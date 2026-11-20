//! Background maintenance for TaskForge.
//!
//! The [`Reaper`] is what turns "a worker died" into "the job runs again". It
//! never asks whether a worker is alive - that question has no reliable
//! answer. It only asks whether a lease has expired, which the store can
//! answer exactly.
//!
//! It keeps no state of its own, so a reaper that crashes and restarts picks
//! up exactly where the last one left off, and any number can run at once:
//! each expired lease is handled by exactly one of them. Workers run one by
//! default, so a deployment with workers can never be left without one.
//!
//! The same loop also purges finished jobs past their retention period.

use std::sync::Arc;
use std::time::Duration;

use taskforge_metrics::Metrics;
use taskforge_storage::{JobStore, Reaped, StoreError};
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How often to look for expired leases. Recovery time after a worker
    /// crash is at most `lease + interval` plus the retry backoff.
    pub interval: Duration,
    /// Most leases recovered (or jobs purged) by one statement.
    pub batch: u32,
    /// Finished jobs older than this are deleted. `None` keeps them forever.
    pub retention: Option<Duration>,
    /// How often to purge.
    pub purge_interval: Duration,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            batch: 100,
            retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            purge_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Reaper<S> {
    store: S,
    config: ReaperConfig,
    metrics: Arc<Metrics>,
}

impl<S: JobStore> Reaper<S> {
    pub fn new(store: S, config: ReaperConfig) -> Self {
        Self {
            store,
            config,
            metrics: Metrics::shared(),
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Recover every lease that has expired by now.
    pub async fn tick(&self) -> Result<Reaped, StoreError> {
        let mut total = Reaped::default();
        loop {
            let reaped = self.store.reap_expired(self.config.batch).await?;
            self.metrics.reaped(reaped);
            total.requeued += reaped.requeued;
            total.dead += reaped.dead;
            total.cancelled += reaped.cancelled;
            // A short batch means there is nothing left for now.
            if reaped.total() < u64::from(self.config.batch) {
                return Ok(total);
            }
        }
    }

    /// Delete finished jobs past the retention period.
    pub async fn purge(&self) -> Result<u64, StoreError> {
        let Some(retention) = self.config.retention else {
            return Ok(0);
        };
        let mut total = 0;
        loop {
            let n = self
                .store
                .purge_finished(retention, self.config.batch)
                .await?;
            self.metrics.purged(n);
            total += n;
            if n < u64::from(self.config.batch) {
                return Ok(total);
            }
        }
    }

    /// Run until `shutdown` becomes `true` or its sender is dropped. A failed
    /// pass (the database being away) is logged and tried again next interval.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        tracing::info!(interval = ?self.config.interval, retention = ?self.config.retention, "reaper started");
        let mut database_down = false;
        let mut next_purge = Instant::now();
        while !*shutdown.borrow() {
            let mut pass = self.tick().await.map(|reaped| {
                if reaped.total() > 0 {
                    tracing::warn!(
                        requeued = reaped.requeued,
                        dead = reaped.dead,
                        cancelled = reaped.cancelled,
                        "recovered jobs from expired leases"
                    );
                }
            });
            if pass.is_ok() && Instant::now() >= next_purge {
                next_purge = Instant::now() + self.config.purge_interval;
                pass = self.purge().await.map(|n| {
                    if n > 0 {
                        tracing::info!(purged = n, "deleted finished jobs past retention");
                    }
                });
            }
            match pass {
                Ok(()) if database_down => {
                    tracing::info!("reaper: database is reachable again");
                    database_down = false;
                }
                Ok(()) => {}
                Err(err) => {
                    // Log the transition, not every failed pass.
                    if !database_down {
                        tracing::warn!(error = %err, "reaper pass failed; will keep trying");
                    }
                    database_down = true;
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {}
                _ = shutdown.wait_for(|stop| *stop) => break,
            }
        }
        tracing::info!("reaper stopped");
    }
}
