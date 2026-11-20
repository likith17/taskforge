//! `taskforge` - one executable, several roles.
//!
//! ```text
//! taskforge migrate     apply database migrations and exit
//! taskforge serve       run the HTTP API
//! taskforge worker      run a worker
//! taskforge scheduler   run the lease reaper (recovers jobs of dead workers)
//! taskforge all         API, scheduler and one worker in a single process
//! ```

mod config;

use std::process::ExitCode;

use anyhow::Context;
use taskforge_api::ApiConfig;
use taskforge_scheduler::{Reaper, ReaperConfig};
use taskforge_storage::JobStore;
use taskforge_worker::{HandlerRegistry, Worker, WorkerConfig, handlers};
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
    let pool = taskforge_storage::connect_lazy(&config.database_url, config.db_max_connections)
        .context("invalid DATABASE_URL")?;
    let store = JobStore::new(pool.clone()).with_retry_policy(config.retry);
    let registry = handlers::builtin();

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
            serve(&config, store.clone(), &registry, stopped.clone()).await
        } else {
            Ok(())
        }
    };
    let worker = async {
        if role.runs(Role::Worker) {
            let worker = Worker::new(
                store.clone(),
                registry.clone(),
                WorkerConfig {
                    worker_id: new_worker_id(),
                    queues: config.queues.clone(),
                    poll_interval: config.poll_interval,
                    lease: config.lease,
                    concurrency: config.concurrency,
                    shutdown_grace: config.shutdown_grace,
                },
            );
            worker.run(stopped.clone()).await;
        }
    };
    let scheduler = async {
        if role.runs(Role::Scheduler) {
            let reaper = Reaper::new(
                store.clone(),
                ReaperConfig {
                    interval: config.reap_interval,
                    ..ReaperConfig::default()
                },
            );
            reaper.run(stopped.clone()).await;
        }
    };

    let (api_result, (), ()) = tokio::join!(api, worker, scheduler);
    pool.close().await;
    api_result
}

async fn serve(
    config: &Config,
    store: JobStore,
    registry: &HandlerRegistry,
    mut stopped: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let app = taskforge_api::router(
        store,
        ApiConfig {
            max_body_bytes: config.max_body_bytes,
            known_kinds: Some(registry.kinds()),
        },
    );
    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("binding {}", config.bind))?;
    tracing::info!(addr = %listener.local_addr()?, "API listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            // Resolves on `true` or when the sender is gone.
            let _ = stopped.wait_for(|stop| *stop).await;
        })
        .await
        .context("HTTP server failed")
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
