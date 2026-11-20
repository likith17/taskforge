//! Persistence for TaskForge.
//!
//! [`JobStore`] is the boundary between scheduling logic and I/O. It has two
//! implementations:
//!
//! * [`PgJobStore`] - PostgreSQL. Durable; what production uses. Every SQL
//!   statement in the system lives in [`pg`].
//! * [`MemJobStore`] - in memory, with an injected clock and jitter source.
//!   For tests and deterministic simulation.
//!
//! The conformance tests run the same scenarios against both.

mod error;
mod mem;
mod pg;
mod traits;
mod types;

#[cfg(test)]
mod conformance;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_reliability;

use std::str::FromStr;
use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};

pub use error::StoreError;
pub use mem::MemJobStore;
pub use pg::PgJobStore;
pub use traits::JobStore;
pub use types::{
    CancelOutcome, Claim, ClaimedJob, Heartbeat, ListFilter, QueueStats, Reaped, Submitted,
};

/// Migrations embedded in the binary at compile time.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// How a process connects to PostgreSQL.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections: u32,
    /// How long to wait for a free connection before giving up.
    pub acquire_timeout: Duration,
    /// Server-side limit on any single statement. Without it, a hung query
    /// (lock wait, overloaded server) could stall a worker past its lease;
    /// with it, the statement fails, the worker sees an error it can retry,
    /// and the lease logic stays in charge.
    pub statement_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            acquire_timeout: Duration::from_secs(3),
            statement_timeout: Duration::from_secs(5),
        }
    }
}

/// Open a connection pool.
///
/// Connections are opened lazily, so this succeeds even while the database is
/// down; callers find out on first use, where the acquire timeout turns an
/// outage into a prompt error instead of a hung request.
pub fn connect_lazy(database_url: &str, config: &PoolConfig) -> Result<PgPool, StoreError> {
    let options = PgConnectOptions::from_str(database_url)?.options([(
        "statement_timeout",
        config.statement_timeout.as_millis().max(1),
    )]);
    Ok(PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.acquire_timeout)
        .connect_lazy_with(options))
}

/// Apply pending migrations. Safe to call from several processes at once:
/// sqlx serializes migration runs with an advisory lock.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    MIGRATOR.run(pool).await?;
    Ok(())
}
