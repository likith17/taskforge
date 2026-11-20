//! `taskforge` - one executable, several roles.
//!
//! ```text
//! taskforge migrate     apply database migrations and exit
//! taskforge serve       run the HTTP API (and /metrics)
//! taskforge worker      run a worker (with its own lease reaper by default)
//! taskforge scheduler   run a standalone lease reaper and purger
//! taskforge all         API, reaper and one worker in a single process
//! ```

mod chaos;
mod config;

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use taskforge_api::ApiConfig;
use taskforge_metrics::Metrics;
use taskforge_scheduler::{Reaper, ReaperConfig};
use taskforge_storage::{PgJobStore, PoolConfig};
use taskforge_worker::{Worker, WorkerConfig, handlers};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::config::Config;

const USAGE: &str = "usage: taskforge <migrate|serve|worker|scheduler|all>";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Migrate,
    Serve,
    Worker,
    Scheduler,
    All,
}

impl Role {
    fn parse(arg: &str) -> Option<Self> {
        Some(match arg {
            "migrate" => Role::Migrate,
            "serve" => Role::Serve,
            "worker" => Role::Worker,
            "scheduler" => Role::Scheduler,
            "all" => Role::All,
            _ => return None,
        })
    }

    fn runs(self, part: Role) -> bool {
        self == part || self == Role::All
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let role = match (args.next().as_deref().and_then(Role::parse), args.next()) {
        (Some(role), None) => role,
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("configuration error: {err:#}");
            return ExitCode::from(2);
        }
    };
    init_logging(config.log_json);

    match run(role, config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = format!("{err:#}"), "taskforge exited with an error");
            ExitCode::FAILURE
        }
    }
}

async fn run(role: Role, config: Config) -> anyhow::Result<()> {
    let pool_config = PoolConfig {
        max_connections: config.db_max_connections,
        statement_timeout: config.statement_timeout,
        ..PoolConfig::default()
    };
    let pool = taskforge_storage::connect_lazy(&config.database_url, &pool_config)
        .context("invalid DATABASE_URL")?;
    let store = PgJobStore::new(pool.clone()).with_retry_policy(config.retry);
    let metrics = Metrics::shared();

    let mut registry = handlers::builtin();
    if config.chaos_handler {
        registry = chaos::register(registry, pool.clone());
    }

    // The API owns the schema. Workers and schedulers never migrate: they can
    // run with credentials that have no DDL rights.
    if matches!(role, Role::Migrate | Role::Serve | Role::All) {
        taskforge_storage::migrate(&pool)
            .await
            .context("applying database migrations")?;
        tracing::info!("database schema is up to date");
    }
    if role == Role::Migrate {
        return Ok(());
    }

    let (stop, stopped) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown requested; finishing in-flight work");
        let _ = stop.send(true);
    });

    let api = async {
        if role.runs(Role::Serve) {
            let app = taskforge_api::router(
                store.clone(),
                ApiConfig {
                    max_body_bytes: config.max_body_bytes,
                    known_kinds: Some(registry.kinds()),
                    metrics: metrics.clone(),
                },
            );
            serve(config.bind, app, stopped.clone()).await
        } else if let Some(bind) = config.metrics_bind {
            serve(bind, metrics_only(metrics.clone()), stopped.clone()).await
        } else {
            Ok(())
        }
    };

    let worker = async {
        if role.runs(Role::Worker) {
            // Lease renewal gets a separate, small pool: if claims and settles
            // exhaust the main pool, renewals must still go out, or healthy
            // jobs lose their leases and run twice.
            let heartbeat_pool = taskforge_storage::connect_lazy(
                &config.database_url,
                &PoolConfig {
                    max_connections: 2,
                    ..pool_config.clone()
                },
            )?;
            let worker = Worker::new(
                store.clone(),
                registry.clone(),
                WorkerConfig {
                    worker_id: config.worker_id.clone().unwrap_or_else(new_worker_id),
                    queues: config.queues.clone(),
                    poll_interval: config.poll_interval,
                    lease: config.lease,
                    concurrency: config.concurrency,
                    shutdown_grace: config.shutdown_grace,
                },
            )
            .with_heartbeat_store(
                PgJobStore::new(heartbeat_pool.clone()).with_retry_policy(config.retry),
            )
            .with_metrics(metrics.clone());
            worker.run(stopped.clone()).await;
            heartbeat_pool.close().await;
        }
        anyhow::Ok(())
    };

    // Every worker process reaps by default, so recovery never depends on a
    // separate scheduler process being up. Many reapers are safe: each expired
    // lease is locked by exactly one of them.
    let runs_reaper = role.runs(Role::Scheduler) || (role == Role::Worker && config.worker_reaper);
    let scheduler = async {
        if runs_reaper {
            let reaper = Reaper::new(
                store.clone(),
                ReaperConfig {
                    interval: config.reap_interval,
                    retention: config.retention,
                    ..ReaperConfig::default()
                },
            )
            .with_metrics(metrics.clone());
            reaper.run(stopped.clone()).await;
        }
    };

    let (api_result, worker_result, ()) = tokio::join!(api, worker, scheduler);
    pool.close().await;
    api_result.and(worker_result)
}

async fn serve(
    bind: SocketAddr,
    app: Router,
    mut stopped: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!(addr = %listener.local_addr()?, "HTTP listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            // Resolves on `true` or when the sender is gone.
            let _ = stopped.wait_for(|stop| *stop).await;
        })
        .await
        .context("HTTP server failed")
}

/// `/metrics` and `/healthz` for processes that do not run the API.
fn metrics_only(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                async move {
                    (
                        [(
                            header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )],
                        metrics.encode(),
                    )
                        .into_response()
                }
            }),
        )
        .route("/healthz", get(|| async { "ok" }))
}

/// Unique per process incarnation, so a restarted worker can never be mistaken
/// for its predecessor and settle a job under an old claim.
fn new_worker_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "worker".to_owned());
    let nonce = uuid::Uuid::now_v7().simple().to_string();
    format!(
        "{host}-{}-{}",
        std::process::id(),
        &nonce[nonce.len() - 8..]
    )
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

fn init_logging(json: bool) {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}
