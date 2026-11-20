//! Configuration from environment variables (twelve-factor style). Secrets
//! such as the database password only ever arrive this way, never from files
//! in the repository.

use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, bail};
use taskforge_domain::RetryPolicy;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub db_max_connections: u32,
    pub bind: SocketAddr,
    pub max_body_bytes: usize,
    pub queues: Vec<String>,
    pub poll_interval: Duration,
    pub lease: Duration,
    pub concurrency: usize,
    pub shutdown_grace: Duration,
    pub reap_interval: Duration,
    pub retry: RetryPolicy,
    pub log_json: bool,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Reads settings through `lookup`, so tests need not touch the process
    /// environment.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let get = |name: &str| lookup(name).filter(|v| !v.trim().is_empty());

        let database_url = get("DATABASE_URL").context(
            "DATABASE_URL is not set (for local development: \
             postgres://taskforge:taskforge@127.0.0.1:5434/taskforge)",
        )?;

        let queues: Vec<String> = get("TASKFORGE_QUEUES")
            .unwrap_or_else(|| taskforge_domain::DEFAULT_QUEUE.to_owned())
            .split(',')
            .map(|q| q.trim().to_owned())
            .filter(|q| !q.is_empty())
            .collect();
        if queues.is_empty() {
            bail!("TASKFORGE_QUEUES must name at least one queue");
        }

        let log_json = match get("TASKFORGE_LOG_FORMAT").as_deref() {
            None | Some("text") => false,
            Some("json") => true,
            Some(other) => bail!("TASKFORGE_LOG_FORMAT must be 'text' or 'json', got {other:?}"),
        };

        let positive = |name: &str, default: u64| -> anyhow::Result<u64> {
            let value: u64 = parse(&get, name, default)?;
            if value == 0 {
                bail!("{name} must be positive");
            }
            Ok(value)
        };

        let concurrency = positive("TASKFORGE_CONCURRENCY", 4)?;
        let db_max_connections: u32 = parse(&get, "TASKFORGE_DB_MAX_CONNECTIONS", 10)?;
        if db_max_connections == 0 {
            bail!("TASKFORGE_DB_MAX_CONNECTIONS must be positive");
        }

        let retry_base = Duration::from_millis(positive("TASKFORGE_RETRY_BASE_MS", 1000)?);
        let retry_cap = Duration::from_secs(positive("TASKFORGE_RETRY_CAP_SECS", 300)?);
        if retry_cap < retry_base {
            bail!("TASKFORGE_RETRY_CAP_SECS must not be smaller than TASKFORGE_RETRY_BASE_MS");
        }

        Ok(Config {
            database_url,
            db_max_connections,
            bind: parse(
                &get,
                "TASKFORGE_BIND",
                SocketAddr::from(([127, 0, 0, 1], 8080)),
            )?,
            max_body_bytes: parse(
                &get,
                "TASKFORGE_MAX_BODY_BYTES",
                taskforge_domain::MAX_PAYLOAD_BYTES + 1024,
            )?,
            queues,
            poll_interval: Duration::from_millis(positive("TASKFORGE_POLL_INTERVAL_MS", 250)?),
            lease: Duration::from_secs(positive("TASKFORGE_LEASE_SECS", 30)?),
            concurrency: usize::try_from(concurrency).unwrap_or(usize::MAX),
            shutdown_grace: Duration::from_secs(parse(&get, "TASKFORGE_SHUTDOWN_GRACE_SECS", 30)?),
            reap_interval: Duration::from_millis(positive("TASKFORGE_REAP_INTERVAL_MS", 1000)?),
            retry: RetryPolicy::new(retry_base, retry_cap),
            log_json,
        })
    }
}

fn parse<T>(get: &impl Fn(&str) -> Option<String>, name: &str, default: T) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    match get(name) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|e| anyhow::anyhow!("{name}={raw:?} is invalid: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(vars: &[(&str, &str)]) -> anyhow::Result<Config> {
        Config::from_lookup(|name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_owned())
        })
    }

    #[test]
    fn defaults_apply_when_only_the_database_is_given() {
        let c = config(&[("DATABASE_URL", "postgres://x")]).unwrap();
        assert_eq!(c.bind.to_string(), "127.0.0.1:8080");
        assert_eq!(c.queues, ["default"]);
        assert_eq!(c.poll_interval, Duration::from_millis(250));
        assert_eq!(c.lease, Duration::from_secs(30));
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.shutdown_grace, Duration::from_secs(30));
        assert_eq!(c.reap_interval, Duration::from_secs(1));
        assert_eq!(c.retry, RetryPolicy::default());
        assert!(!c.log_json);
    }

    #[test]
    fn database_url_is_required() {
        assert!(
            config(&[])
                .unwrap_err()
                .to_string()
                .contains("DATABASE_URL")
        );
        assert!(config(&[("DATABASE_URL", "  ")]).is_err());
    }

    #[test]
    fn values_are_parsed_and_validated() {
        let c = config(&[
            ("DATABASE_URL", "postgres://x"),
            ("TASKFORGE_QUEUES", "mail, reports ,"),
            ("TASKFORGE_BIND", "0.0.0.0:9000"),
            ("TASKFORGE_LOG_FORMAT", "json"),
            ("TASKFORGE_POLL_INTERVAL_MS", "50"),
            ("TASKFORGE_CONCURRENCY", "16"),
            ("TASKFORGE_LEASE_SECS", "5"),
            ("TASKFORGE_SHUTDOWN_GRACE_SECS", "0"),
            ("TASKFORGE_RETRY_BASE_MS", "200"),
            ("TASKFORGE_RETRY_CAP_SECS", "10"),
        ])
        .unwrap();
        assert_eq!(c.queues, ["mail", "reports"]);
        assert_eq!(c.bind.port(), 9000);
        assert!(c.log_json);
        assert_eq!(c.poll_interval, Duration::from_millis(50));
        assert_eq!(c.concurrency, 16);
        assert_eq!(c.lease, Duration::from_secs(5));
        assert_eq!(c.shutdown_grace, Duration::ZERO);
        assert_eq!(
            c.retry,
            RetryPolicy::new(Duration::from_millis(200), Duration::from_secs(10))
        );

        for bad in [
            ("TASKFORGE_BIND", "not-an-address"),
            ("TASKFORGE_POLL_INTERVAL_MS", "0"),
            ("TASKFORGE_LEASE_SECS", "-5"),
            ("TASKFORGE_LEASE_SECS", "0"),
            ("TASKFORGE_CONCURRENCY", "0"),
            ("TASKFORGE_DB_MAX_CONNECTIONS", "0"),
            ("TASKFORGE_REAP_INTERVAL_MS", "soon"),
            ("TASKFORGE_LOG_FORMAT", "xml"),
            ("TASKFORGE_QUEUES", " , "),
        ] {
            let err = config(&[("DATABASE_URL", "postgres://x"), bad]).unwrap_err();
            assert!(err.to_string().contains(bad.0), "{bad:?}: {err}");
        }

        // A cap below the base would make every delay shrink to the cap.
        let err = config(&[
            ("DATABASE_URL", "postgres://x"),
            ("TASKFORGE_RETRY_BASE_MS", "5000"),
            ("TASKFORGE_RETRY_CAP_SECS", "1"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("TASKFORGE_RETRY_CAP_SECS"));
    }
}
