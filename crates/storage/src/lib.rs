//! PostgreSQL persistence for TaskForge.
//!
//! Every SQL statement in the system lives in this crate. Each job state
//! transition is a single guarded `UPDATE`, so it is atomic without explicit
//! transactions and cannot be half-applied by a crash.

mod error;
mod store;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_reliability;

use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub use error::StoreError;
pub use store::{
    CancelOutcome, Claim, ClaimedJob, Heartbeat, JobStore, ListFilter, Reaped, Submitted,
};

/// Migrations embedded in the binary at compile time.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Open a connection pool.
///
/// Connections are opened lazily, so this succeeds even while the database is
/// down; callers find out on first use, where the short acquire timeout turns
/// an outage into a prompt error instead of a hung request.
pub fn connect_lazy(database_url: &str, max_connections: u32) -> Result<PgPool, StoreError> {
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(3))
        .connect_lazy(database_url)?)
}

/// Apply pending migrations. Safe to call from several processes at once:
/// sqlx serializes migration runs with an advisory lock.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    MIGRATOR.run(pool).await?;
    Ok(())
}
