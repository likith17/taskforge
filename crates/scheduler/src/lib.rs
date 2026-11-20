//! Background maintenance for TaskForge.
//!
//! The [`Reaper`] is what turns "a worker died" into "the job runs again". It
//! never asks whether a worker is alive - that question has no reliable
//! answer. It only asks whether a lease has expired, which the database can
//! answer exactly.
//!
//! Any number of reapers may run at once (one per API process, say): each
//! expired lease is locked and handled by exactly one of them.

use std::time::Duration;

use taskforge_storage::{JobStore, Reaped, StoreError};
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How often to look for expired leases. Together with the lease length
    /// this bounds recovery time: a dead worker's job is back in the queue
    /// within `lease + interval`.
    pub interval: Duration,
    /// Most leases recovered by one statement.
    pub batch: u32,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            batch: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Reaper {
    store: JobStore,
    config: ReaperConfig,
}

impl Reaper {
    pub fn new(store: JobStore, config: ReaperConfig) -> Self {
        Self { store, config }
    }

    /// Recover every lease that has expired by now.
    pub async fn tick(&self) -> Result<Reaped, StoreError> {
        let mut total = Reaped::default();
        loop {
            let reaped = self.store.reap_expired(self.config.batch).await?;
            total.requeued += reaped.requeued;
            total.dead += reaped.dead;
            total.cancelled += reaped.cancelled;
            // A short batch means there is nothing left for now.
            if reaped.total() < u64::from(self.config.batch) {
                return Ok(total);
            }
        }
    }

    /// Run until `shutdown` becomes `true` or its sender is dropped. A failed
    /// pass (the database being away) is logged and tried again next interval.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        tracing::info!(interval = ?self.config.interval, "lease reaper started");
        let mut database_down = false;
        while !*shutdown.borrow() {
            match self.tick().await {
                Ok(reaped) => {
                    if database_down {
                        tracing::info!("lease reaper: database is reachable again");
                        database_down = false;
                    }
                    if reaped.total() > 0 {
                        tracing::warn!(
                            requeued = reaped.requeued,
                            dead = reaped.dead,
                            cancelled = reaped.cancelled,
                            "recovered jobs from expired leases"
                        );
                    }
                }
                Err(err) => {
                    // Log the transition, not every failed pass.
                    if !database_down {
                        tracing::warn!(error = %err, "lease reaper pass failed; will keep trying");
                    }
                    database_down = true;
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {}
                _ = shutdown.wait_for(|stop| *stop) => break,
            }
        }
        tracing::info!("lease reaper stopped");
    }
}
