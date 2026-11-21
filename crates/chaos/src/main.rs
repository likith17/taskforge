//! `taskforge-chaos` - fault injection and benchmarks with real processes.
//!
//! ```text
//! taskforge-chaos chaos [options]   kill workers (and the scheduler) mid-job, verify invariants
//! taskforge-chaos bench [options]   throughput at several worker counts
//! taskforge-chaos claims [options]  claim strategies compared: naive, FOR UPDATE, SKIP LOCKED
//! ```
//!
//! Both start real `taskforge` processes against a fresh database and kill
//! them with `Child::kill` (SIGKILL on Unix, TerminateProcess on Windows): no
//! signal handler runs, nothing is released, the process just stops. Every
//! number reported is computed from what the database recorded; nothing is
//! estimated. Results are printed and written as JSON to `experiments/results/`.
//!
//! Needs `docker compose up -d db` and a release build of both binaries:
//! `cargo build --release -p taskforge -p taskforge-chaos`.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use taskforge_domain::{IdempotencyKey, JobSubmission, NewJob, SplitMix64};
use taskforge_storage::{JobStore, PgJobStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const USAGE: &str =
    "usage: taskforge-chaos <chaos|bench> [--option value ...]  (see crates/chaos/src/main.rs)";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_default();
    let options = Options::parse(args)?;
    match command.as_str() {
        "chaos" => chaos(&options).await,
        "bench" => bench(&options).await,
        "claims" => claims(&options).await,
        _ => bail!("{USAGE}"),
    }
}

// ------------------------------------------------------------------ options

struct Options(HashMap<String, String>);

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let args: Vec<String> = args.collect();
        let mut map = HashMap::new();
        let mut i = 0;
        while i < args.len() {
            let key = args[i]
                .strip_prefix("--")
                .with_context(|| format!("expected --option, got {:?}", args[i]))?;
            let value = args
                .get(i + 1)
                .with_context(|| format!("--{key} needs a value"))?;
            map.insert(key.to_owned(), value.clone());
            i += 2;
        }
        Ok(Self(map))
    }

    fn get<T: std::str::FromStr>(&self, key: &str, default: T) -> anyhow::Result<T>
    where
        T::Err: std::fmt::Display,
    {
        match self.0.get(key) {
            None => Ok(default),
            Some(raw) => raw
                .parse()
                .map_err(|e| anyhow::anyhow!("--{key} {raw:?}: {e}")),
        }
    }

    fn string(&self, key: &str, default: &str) -> String {
        self.0
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_owned())
    }
}

// -------------------------------------------------------------- environment

struct Env {
    admin_url: String,
    bin: PathBuf,
    logs: PathBuf,
}

impl Env {
    fn new(options: &Options, run_name: &str) -> anyhow::Result<Self> {
        let admin_url = options.string(
            "admin-url",
            &std::env::var("CHAOS_ADMIN_URL").unwrap_or_else(|_| {
                "postgres://taskforge:taskforge@127.0.0.1:5434/postgres".to_owned()
            }),
        );
        let bin = match options.0.get("bin") {
            Some(path) => PathBuf::from(path),
            None => {
                let exe = std::env::current_exe()?;
                let dir = exe.parent().context("no executable directory")?;
                dir.join(format!("taskforge{}", std::env::consts::EXE_SUFFIX))
            }
        };
        if !bin.exists() {
            bail!(
                "{} not found; build it with `cargo build --release -p taskforge`",
                bin.display()
            );
        }
        let logs = PathBuf::from("experiments").join("logs").join(run_name);
        fs::create_dir_all(&logs)?;
        Ok(Self {
            admin_url,
            bin,
            logs,
        })
    }

    fn database_url(&self, db: &str) -> String {
        match self.admin_url.rsplit_once('/') {
            Some((base, _)) => format!("{base}/{db}"),
            None => format!("{}/{db}", self.admin_url),
        }
    }

    /// Drop and recreate `db`, apply migrations, and create the ledger.
    async fn fresh_database(&self, db: &str) -> anyhow::Result<PgPool> {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
            .context("connecting to PostgreSQL (is `docker compose up -d db` running?)")?;
        sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{db}" WITH (FORCE)"#))
            .execute(&admin)
            .await?;
        sqlx::query(&format!(r#"CREATE DATABASE "{db}""#))
            .execute(&admin)
            .await?;
        admin.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(&self.database_url(db))
            .await?;
        taskforge_storage::migrate(&pool).await?;
        sqlx::query(
            "CREATE TABLE chaos_ledger (
                 job_id uuid NOT NULL, attempt int NOT NULL, worker_id text NOT NULL,
                 event text NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())",
        )
        .execute(&pool)
        .await?;
        Ok(pool)
    }

    fn spawn(&self, role: &str, name: &str, env: &[(&str, String)]) -> anyhow::Result<Proc> {
        let log = File::create(self.logs.join(format!("{name}.log")))?;
        let child = Command::new(&self.bin)
            .arg(role)
            .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()
            .with_context(|| format!("starting {}", self.bin.display()))?;
        Ok(Proc {
            name: name.to_owned(),
            child,
        })
    }
}

struct Proc {
    name: String,
    child: Child,
}

impl Proc {
    /// SIGKILL / TerminateProcess, then reap it. Returns once it is gone.
    fn kill(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.name
    }
}

// --------------------------------------------------------------- statistics

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    // Nearest-rank: the smallest sample with at least p% of samples at or below it.
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

fn summary(mut samples: Vec<f64>) -> Value {
    samples.sort_by(f64::total_cmp);
    let n = samples.len();
    let mean = if n == 0 {
        f64::NAN
    } else {
        samples.iter().sum::<f64>() / n as f64
    };
    json!({
        "n": n,
        "mean": round3(mean),
        "p50": round3(percentile(&samples, 50.0)),
        "p95": round3(percentile(&samples, 95.0)),
        "p99": round3(percentile(&samples, 99.0)),
        "max": round3(samples.last().copied().unwrap_or(f64::NAN)),
    })
}

fn round3(x: f64) -> Value {
    if x.is_finite() {
        json!((x * 1000.0).round() / 1000.0)
    } else {
        Value::Null
    }
}

fn save(name: &str, report: &Value) -> anyhow::Result<PathBuf> {
    let dir = Path::new("experiments").join("results");
    fs::create_dir_all(dir)?;
    let path = Path::new("experiments")
        .join("results")
        .join(format!("{name}.json"));
    fs::write(&path, serde_json::to_string_pretty(report)?)?;
    Ok(path)
}

async fn submit_jobs(
    store: &PgJobStore,
    count: usize,
    make: impl Fn(usize) -> (Value, u32) + Send + Sync + 'static,
    twice: bool,
) -> anyhow::Result<usize> {
    // Each job is submitted with an idempotency key; with `twice`, every
    // submission is sent a second time concurrently, like a client retrying.
    let mut duplicates = 0;
    for chunk in (0..count).collect::<Vec<_>>().chunks(64) {
        let mut tasks = Vec::new();
        for &i in chunk {
            // Built once per job, so a repeat is byte-for-byte the same request.
            let (spec, max_attempts) = make(i);
            for _ in 0..if twice { 2 } else { 1 } {
                let store = store.clone();
                let spec = spec.clone();
                tasks.push(tokio::spawn(async move {
                    let kind = spec["kind"].as_str().unwrap_or("chaos").to_owned();
                    let new = NewJob::try_from(JobSubmission {
                        kind,
                        payload: Some(spec["payload"].clone()),
                        max_attempts: Some(max_attempts),
                        ..Default::default()
                    })?
                    .with_idempotency_key(IdempotencyKey::parse(&format!("job-{i}"))?);
                    anyhow::Ok(store.submit(&new).await?.is_duplicate())
                }));
            }
        }
        for task in tasks {
            duplicates += usize::from(task.await??);
        }
    }
    Ok(duplicates)
}

async fn unfinished(pool: &PgPool) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE state NOT IN ('succeeded', 'dead', 'cancelled')",
    )
    .fetch_one(pool)
    .await?)
}

// -------------------------------------------------------------------- chaos

/// Options (all optional):
/// `--name`, `--jobs 400`, `--workers 6`, `--concurrency 2`,
/// `--kill-every-ms 700`, `--scheduler-kill-every-ms 0` (0 = never),
/// `--scheduler-down-ms 1000`, `--worker-reaper false`, `--fail-every 20`,
/// `--work-ms-max 400`, `--lease-ms 2000`, `--reap-ms 250`, `--seed 1`,
/// `--timeout-secs 600`.
async fn chaos(o: &Options) -> anyhow::Result<()> {
    let name = o.string("name", "chaos");
    let jobs: usize = o.get("jobs", 400)?;
    let workers: usize = o.get("workers", 6)?;
    let concurrency: usize = o.get("concurrency", 2)?;
    let kill_every = Duration::from_millis(o.get("kill-every-ms", 700)?);
    let scheduler_kill_every_ms: u64 = o.get("scheduler-kill-every-ms", 0)?;
    let scheduler_down = Duration::from_millis(o.get("scheduler-down-ms", 1000)?);
    let worker_reaper: bool = o.get("worker-reaper", false)?;
    let fail_every: usize = o.get("fail-every", 20)?;
    let work_ms_max: u64 = o.get("work-ms-max", 400)?;
    let lease_ms: u64 = o.get("lease-ms", 2000)?;
    let reap_ms: u64 = o.get("reap-ms", 250)?;
    let seed: u64 = o.get("seed", 1)?;
    let timeout = Duration::from_secs(o.get("timeout-secs", 600)?);

    let env = Env::new(o, &name)?;
    let db = "taskforge_chaos";
    let pool = env.fresh_database(db).await?;
    let store = PgJobStore::new(pool.clone());
    let rng = Arc::new(SplitMix64::seeded(seed));

    // Deliberately failing jobs get 3 attempts and must end dead-lettered.
    // Everything else gets the maximum budget, so that no amount of bad luck
    // with kills dead-letters a healthy job.
    println!("submitting {jobs} jobs, each twice with the same idempotency key");
    let work_rng = rng.clone();
    let duplicates = submit_jobs(
        &store,
        jobs,
        move |i| {
            let fail = fail_every > 0 && i % fail_every == 0;
            let work_ms = 20 + work_rng.next_u64() % work_ms_max.max(1);
            (
                json!({"kind": "chaos", "payload": {"n": i, "work_ms": work_ms, "fail": fail}}),
                if fail { 3 } else { 25 },
            )
        },
        true,
    )
    .await?;
    let expected_dead = if fail_every == 0 {
        0
    } else {
        jobs.div_ceil(fail_every)
    };

    let base_env = |id: &str| -> Vec<(&'static str, String)> {
        vec![
            ("DATABASE_URL", env.database_url(db)),
            ("TASKFORGE_WORKER_ID", id.to_owned()),
            ("TASKFORGE_CHAOS_HANDLER", "true".to_owned()),
            ("TASKFORGE_LEASE_MS", lease_ms.to_string()),
            ("TASKFORGE_REAP_INTERVAL_MS", reap_ms.to_string()),
            ("TASKFORGE_RETRY_BASE_MS", "250".to_owned()),
            ("TASKFORGE_RETRY_CAP_MS", "2000".to_owned()),
            ("TASKFORGE_POLL_INTERVAL_MS", "50".to_owned()),
            ("TASKFORGE_CONCURRENCY", concurrency.to_string()),
            (
                "TASKFORGE_DB_MAX_CONNECTIONS",
                (concurrency + 2).to_string(),
            ),
            ("TASKFORGE_WORKER_REAPER", worker_reaper.to_string()),
            ("TASKFORGE_LOG_FORMAT", "json".to_owned()),
            ("RUST_LOG", "warn".to_owned()),
        ]
    };

    let mut generation = 0usize;
    let mut spawn_worker = |slot: usize| {
        generation += 1;
        let id = format!("w{slot}-g{generation}");
        env.spawn("worker", &id, &base_env(&id))
    };
    let mut fleet: Vec<Proc> = (0..workers)
        .map(&mut spawn_worker)
        .collect::<Result<_, _>>()?;
    let mut scheduler_gen = 1;
    let mut scheduler = Some(env.spawn("scheduler", "scheduler-g1", &base_env("scheduler"))?);

    println!(
        "{workers} workers x {concurrency} slots + 1 scheduler running; killing a worker every {kill_every:?}"
    );
    let started = Instant::now();
    let mut kills: Vec<Value> = Vec::new();
    let mut orphans: Vec<(Uuid, i32, DateTime<Utc>)> = Vec::new();
    let mut scheduler_kills = 0;
    let mut next_kill = Instant::now() + kill_every;
    let mut next_scheduler_kill = (scheduler_kill_every_ms > 0)
        .then(|| Instant::now() + Duration::from_millis(scheduler_kill_every_ms));
    let mut scheduler_back_at: Option<Instant> = None;
    let mut last_print = Instant::now();

    loop {
        let left = unfinished(&pool).await?;
        if left == 0 {
            break;
        }
        if started.elapsed() > timeout {
            println!("TIMEOUT with {left} unfinished jobs");
            break;
        }
        if last_print.elapsed() > Duration::from_secs(2) {
            println!(
                "  t+{:>5.1}s  unfinished {left:>5}  worker kills {}  scheduler kills {scheduler_kills}",
                started.elapsed().as_secs_f64(),
                kills.len()
            );
            last_print = Instant::now();
        }

        if Instant::now() >= next_kill {
            next_kill = Instant::now() + kill_every;
            let slot = (rng.next_u64() % fleet.len() as u64) as usize;
            let victim = fleet.remove(slot).kill();
            // The process is dead, so its rows can no longer change until a
            // reaper takes them back (at least a lease away): this read is an
            // exact list of what it orphaned. The kill time is taken from the
            // database clock, the same clock as `started_at` below.
            let (killed_at,): (DateTime<Utc>,) = sqlx::query_as("SELECT clock_timestamp()")
                .fetch_one(&pool)
                .await?;
            let held: Vec<(Uuid, i32)> = sqlx::query_as(
                "SELECT id, attempt FROM jobs WHERE state = 'running' AND lease_owner = $1",
            )
            .bind(&victim)
            .fetch_all(&pool)
            .await?;
            kills.push(json!({"worker": victim, "at": killed_at, "orphaned": held.len()}));
            orphans.extend(
                held.into_iter()
                    .map(|(id, attempt)| (id, attempt, killed_at)),
            );
            fleet.insert(slot, spawn_worker(slot)?);
        }

        if let Some(at) = next_scheduler_kill {
            if Instant::now() >= at {
                if let Some(proc) = scheduler.take() {
                    proc.kill();
                    scheduler_kills += 1;
                    scheduler_back_at = Some(Instant::now() + scheduler_down);
                }
                next_scheduler_kill =
                    Some(Instant::now() + Duration::from_millis(scheduler_kill_every_ms));
            }
        }
        if scheduler_back_at.is_some_and(|at| Instant::now() >= at) {
            scheduler_back_at = None;
            scheduler_gen += 1;
            scheduler = Some(env.spawn(
                "scheduler",
                &format!("scheduler-g{scheduler_gen}"),
                &base_env("scheduler"),
            )?);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = started.elapsed();
    for proc in fleet.into_iter().chain(scheduler) {
        proc.kill();
    }

    // ------------------------------------------------------------ verify
    let scalar = |sql: &'static str| {
        let pool = pool.clone();
        async move { sqlx::query_scalar::<_, i64>(sql).fetch_one(&pool).await }
    };
    let total = scalar("SELECT count(*) FROM jobs").await?;
    let succeeded = scalar("SELECT count(*) FROM jobs WHERE state = 'succeeded'").await?;
    let dead = scalar("SELECT count(*) FROM jobs WHERE state = 'dead'").await?;
    let dead_unexpected = scalar(
        "SELECT count(*) FROM jobs WHERE state = 'dead' AND (payload->>'fail')::bool IS NOT TRUE",
    )
    .await?;
    let not_terminal =
        scalar("SELECT count(*) FROM jobs WHERE state NOT IN ('succeeded','dead','cancelled')")
            .await?;
    let stuck_leases = scalar("SELECT count(*) FROM jobs WHERE lease_owner IS NOT NULL").await?;
    let open_attempts = scalar("SELECT count(*) FROM job_attempts WHERE outcome IS NULL").await?;
    let multi_committed = scalar(
        "SELECT count(*) FROM (SELECT job_id FROM job_attempts WHERE outcome = 'succeeded'
                               GROUP BY job_id HAVING count(*) > 1) x",
    )
    .await?;
    let wrong_results = scalar(
        "SELECT count(*) FROM jobs WHERE state = 'succeeded'
           AND result IS DISTINCT FROM jsonb_build_object('n', payload->'n')",
    )
    .await?;
    let executions = scalar("SELECT count(*) FROM chaos_ledger WHERE event = 'start'").await?;
    let reexecuted_jobs = scalar(
        "SELECT count(*) FROM (SELECT l.job_id FROM chaos_ledger l JOIN jobs j ON j.id = l.job_id
                               WHERE l.event = 'start' AND (j.payload->>'fail')::bool IS NOT TRUE
                               GROUP BY l.job_id HAVING count(*) > 1) x",
    )
    .await?;
    let unprotected_double_effects = scalar(
        "SELECT count(*) FROM (SELECT job_id FROM chaos_ledger WHERE event = 'finish'
                               GROUP BY job_id HAVING count(*) > 1) x",
    )
    .await?;
    let outcomes: Vec<(Option<String>, i64)> =
        sqlx::query_as("SELECT outcome, count(*) FROM job_attempts GROUP BY 1 ORDER BY 2 DESC")
            .fetch_all(&pool)
            .await?;

    // Recovery: from the kill to the start of the orphaned job's next
    // attempt. An orphan whose attempt was its last (a deliberately failing
    // job on attempt 3) is not re-run: the reaper takes it back and
    // dead-letters it, which also counts as recovered.
    //
    // Two latencies per orphan, both on the database clock:
    // * requeue: kill -> the reaper takes the job back. This is the recovery
    //   mechanism itself (lease expiry + reaper interval).
    // * restart: kill -> its next attempt starts. Adds the retry backoff and
    //   any wait behind other queued jobs, which dominates when the queue is
    //   saturated.
    let mut recovery = Vec::new();
    let mut requeue = Vec::new();
    let mut settled_by_reaper = 0;
    let mut unrecovered = 0;
    for (id, attempt, killed_at) in &orphans {
        let taken_back: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT finished_at FROM job_attempts
             WHERE job_id = $1 AND attempt = $2 AND outcome = 'lease_expired'",
        )
        .bind(id)
        .bind(attempt)
        .fetch_optional(&pool)
        .await?
        .flatten();
        if let Some(at) = taken_back {
            requeue.push((at - *killed_at).num_microseconds().unwrap_or(0) as f64 / 1e6);
        }
        let next: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT started_at FROM job_attempts WHERE job_id = $1 AND attempt = $2",
        )
        .bind(id)
        .bind(attempt + 1)
        .fetch_optional(&pool)
        .await?;
        if let Some(at) = next {
            recovery.push((at - *killed_at).num_microseconds().unwrap_or(0) as f64 / 1e6);
            continue;
        }
        let reaped_and_final: bool = sqlx::query_scalar(
            "SELECT a.outcome = 'lease_expired' AND j.state IN ('dead', 'cancelled')
             FROM job_attempts a JOIN jobs j ON j.id = a.job_id
             WHERE a.job_id = $1 AND a.attempt = $2",
        )
        .bind(id)
        .bind(attempt)
        .fetch_one(&pool)
        .await?;
        if reaped_and_final {
            settled_by_reaper += 1;
        } else {
            unrecovered += 1;
        }
    }

    let healthy = jobs - expected_dead;
    let healthy_succeeded = succeeded as usize;
    let checks = json!({
        "no duplicate jobs from duplicate submissions": total as usize == jobs && duplicates == jobs,
        "every job reached a terminal state": not_terminal == 0,
        "every healthy job succeeded": healthy_succeeded == healthy && dead_unexpected == 0,
        "every deliberately failing job was dead-lettered": dead as usize == expected_dead,
        "exactly one committed result per succeeded job": multi_committed == 0,
        "every result is the right one": wrong_results == 0,
        "no stuck leases": stuck_leases == 0,
        "no attempt left open": open_attempts == 0,
        "every orphaned job was recovered": unrecovered == 0,
    });
    let passed = checks.as_object().unwrap().values().all(|v| v == true);

    let report = json!({
        "experiment": "chaos",
        "name": name,
        "config": {
            "jobs": jobs, "workers": workers, "concurrency_per_worker": concurrency,
            "kill_every_ms": kill_every.as_millis() as u64,
            "scheduler_kill_every_ms": scheduler_kill_every_ms,
            "scheduler_down_ms": scheduler_down.as_millis() as u64,
            "worker_reaper": worker_reaper, "lease_ms": lease_ms, "reap_interval_ms": reap_ms,
            "retry_base_ms": 250, "retry_cap_ms": 2000, "work_ms": format!("20..{}", 20 + work_ms_max),
            "deliberately_failing_jobs": expected_dead, "seed": seed,
        },
        "elapsed_secs": round3(elapsed.as_secs_f64()),
        "kills": { "workers": kills.len(), "scheduler": scheduler_kills, "orphaned_attempts": orphans.len() },
        "results": {
            "jobs_in_database": total,
            "duplicate_submissions_answered": duplicates,
            "succeeded": succeeded,
            "dead_lettered": dead,
            "completion_rate_healthy_jobs": round3(healthy_succeeded as f64 / healthy.max(1) as f64),
            "duplicate_committed_results": multi_committed,
            "handler_executions": executions,
            "healthy_jobs_executed_more_than_once": reexecuted_jobs,
            "jobs_whose_unprotected_side_effect_ran_twice": unprotected_double_effects,
            "stuck_leases": stuck_leases,
            "open_attempts": open_attempts,
            "attempt_outcomes": outcomes.iter().map(|(o, n)| (o.clone().unwrap_or_else(|| "open".into()), json!(n))).collect::<serde_json::Map<_, _>>(),
            "requeue_latency_secs": summary(requeue),
            "restart_latency_secs": summary(recovery),
            "orphans_dead_lettered_by_reaper": settled_by_reaper,
            "unrecovered_orphans": unrecovered,
        },
        "checks": checks,
        "passed": passed,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!("written to {}", save(&name, &report)?.display());
    pool.close().await;
    if !passed {
        bail!("chaos run {name}: invariant check failed");
    }
    Ok(())
}

// -------------------------------------------------------------------- bench

/// Options: `--name`, `--workers-list 1,4,16`, `--jobs 2000`,
/// `--workload noop|io`, `--io-ms 10`, `--repeats 3`.
///
/// Each worker is one process with one execution slot. Jobs are loaded
/// before any worker starts, so the run measures how fast a backlog drains.
async fn bench(o: &Options) -> anyhow::Result<()> {
    let name = o.string("name", "bench");
    let jobs: usize = o.get("jobs", 2000)?;
    let workload = o.string("workload", "noop");
    let io_ms: u64 = o.get("io-ms", 10)?;
    let repeats: usize = o.get("repeats", 3)?;
    let counts: Vec<usize> = o
        .string("workers-list", "1,4,16")
        .split(',')
        .map(|s| s.trim().parse())
        .collect::<Result<_, _>>()?;
    let env = Env::new(o, &name)?;
    let db = "taskforge_bench";

    let mut rows = Vec::new();
    for &workers in &counts {
        let mut runs = Vec::new();
        for repeat in 1..=repeats {
            let pool = env.fresh_database(db).await?;
            let store = PgJobStore::new(pool.clone());
            let workload_kind = workload.clone();
            submit_jobs(
                &store,
                jobs,
                move |i| match workload_kind.as_str() {
                    "io" => (
                        json!({"kind": "sleep", "payload": {"ms": io_ms, "i": i}}),
                        5,
                    ),
                    _ => (json!({"kind": "echo", "payload": {"i": i}}), 5),
                },
                false,
            )
            .await?;

            let mut fleet = Vec::new();
            for w in 0..workers {
                let id = format!("bench-w{w}");
                fleet.push((
                    9300 + w as u16,
                    env.spawn(
                        "worker",
                        &format!("{id}-{workers}x-r{repeat}"),
                        &[
                            ("DATABASE_URL", env.database_url(db)),
                            ("TASKFORGE_WORKER_ID", id.clone()),
                            ("TASKFORGE_CONCURRENCY", "1".into()),
                            ("TASKFORGE_DB_MAX_CONNECTIONS", "2".into()),
                            ("TASKFORGE_POLL_INTERVAL_MS", "50".into()),
                            ("TASKFORGE_WORKER_REAPER", "false".into()),
                            ("TASKFORGE_METRICS_BIND", format!("127.0.0.1:{}", 9300 + w)),
                            ("RUST_LOG", "warn".into()),
                        ],
                    )?,
                ));
            }
            let wall = Instant::now();
            while unfinished(&pool).await? > 0 {
                if wall.elapsed() > Duration::from_secs(900) {
                    bail!("bench run did not finish in 15 minutes");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Claim latency, as measured by the workers themselves.
            let (mut claim_sum, mut claim_count) = (0.0, 0.0);
            for (port, _) in &fleet {
                if let Ok(text) = scrape(*port).await {
                    claim_sum += metric(&text, "taskforge_claim_duration_seconds_sum");
                    claim_count += metric(&text, "taskforge_claim_duration_seconds_count");
                }
            }
            for (_, proc) in fleet {
                proc.kill();
            }

            let (span, succeeded): (f64, i64) = sqlx::query_as(
                "SELECT extract(epoch FROM max(finished_at) - min(started_at))::float8,
                        count(*) FILTER (WHERE state = 'succeeded')
                 FROM jobs",
            )
            .fetch_one(&pool)
            .await?;
            // Time each job spent claimed: handler + settle + the bookkeeping
            // between them, from the database clock.
            let held: Vec<f64> = sqlx::query_scalar(
                "SELECT extract(epoch FROM finished_at - started_at)::float8 FROM jobs",
            )
            .fetch_all(&pool)
            .await?;
            let throughput = succeeded as f64 / span;
            println!(
                "  {workload} x {workers} workers, run {repeat}/{repeats}: {succeeded} jobs in {span:.3}s = {throughput:.0} jobs/s"
            );
            runs.push(json!({
                "repeat": repeat,
                "jobs": succeeded,
                "span_secs": round3(span),
                "throughput_jobs_per_sec": round3(throughput),
                "claim_latency_mean_ms": round3(1000.0 * claim_sum / claim_count.max(1.0)),
                "claims": claim_count,
                "job_held_secs": summary(held),
            }));
            pool.close().await;
        }
        let mut tp: Vec<f64> = runs
            .iter()
            .map(|r| r["throughput_jobs_per_sec"].as_f64().unwrap_or(f64::NAN))
            .collect();
        tp.sort_by(f64::total_cmp);
        rows.push(json!({
            "workers": workers,
            "throughput_median": round3(percentile(&tp, 50.0)),
            "throughput_min": round3(tp[0]),
            "throughput_max": round3(tp[tp.len() - 1]),
            "runs": runs,
        }));
    }

    let report = json!({
        "experiment": "bench",
        "name": name,
        "config": {
            "jobs_per_run": jobs, "workload": workload,
            "io_ms": if workload == "io" { json!(io_ms) } else { Value::Null },
            "repeats": repeats, "concurrency_per_worker": 1, "poll_interval_ms": 50,
            "preloaded": true,
        },
        "results": rows,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!("written to {}", save(&name, &report)?.display());
    Ok(())
}

// ------------------------------------------------------------------- claims

/// The baseline comparison: three ways to take the next job, on a bare table
/// so that nothing but the claim strategy differs.
///
/// * `naive`       - `SELECT` the next queued row, then `UPDATE` it. No lock.
/// * `for_update`  - the same in a transaction with `SELECT ... FOR UPDATE`.
/// * `skip_locked` - TaskForge's single statement with `FOR UPDATE SKIP LOCKED`.
///
/// Every claim is recorded, so double claims (two claimers taking the same
/// job) are counted exactly.
///
/// Options: `--name`, `--jobs 5000`, `--claimers-list 1,4,16`, `--repeats 3`.
async fn claims(o: &Options) -> anyhow::Result<()> {
    let name = o.string("name", "claims");
    let jobs: i64 = o.get("jobs", 5000)?;
    let repeats: usize = o.get("repeats", 3)?;
    let counts: Vec<usize> = o
        .string("claimers-list", "1,4,16")
        .split(',')
        .map(|s| s.trim().parse())
        .collect::<Result<_, _>>()?;
    let env = Env::new(o, &name)?;
    let db = "taskforge_claims";

    let mut rows = Vec::new();
    for strategy in ["naive", "for_update", "skip_locked"] {
        for &claimers in &counts {
            let mut runs = Vec::new();
            for repeat in 1..=repeats {
                let run = claim_run(&env, db, strategy, claimers, jobs).await?;
                println!(
                    "  {strategy:<11} x {claimers:>2} claimers, run {repeat}: {} jobs/s, {} double claims",
                    run["jobs_per_sec"], run["double_claims"]
                );
                runs.push(run);
            }
            let mut tp: Vec<f64> = runs
                .iter()
                .map(|r| r["jobs_per_sec"].as_f64().unwrap_or(f64::NAN))
                .collect();
            tp.sort_by(f64::total_cmp);
            let doubles: i64 = runs
                .iter()
                .map(|r| r["double_claims"].as_i64().unwrap_or(0))
                .sum();
            rows.push(json!({
                "strategy": strategy,
                "claimers": claimers,
                "jobs_per_sec_median": round3(percentile(&tp, 50.0)),
                "jobs_per_sec_min": round3(tp[0]),
                "jobs_per_sec_max": round3(tp[tp.len() - 1]),
                "double_claims_total": doubles,
                "runs": runs,
            }));
        }
    }
    let report = json!({
        "experiment": "claims",
        "name": name,
        "config": {
            "jobs_per_run": jobs,
            "repeats": repeats,
            "claimers": counts,
            "note": "bare table; one claim = the strategy's claim statement(s) plus one INSERT recording it",
        },
        "results": rows,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    println!("written to {}", save(&name, &report)?.display());
    Ok(())
}

async fn claim_run(
    env: &Env,
    db: &str,
    strategy: &'static str,
    claimers: usize,
    jobs: i64,
) -> anyhow::Result<Value> {
    let pool = env.fresh_database(db).await?;
    for ddl in [
        "CREATE TABLE q (id bigint PRIMARY KEY, state text NOT NULL DEFAULT 'queued')",
        "CREATE INDEX q_queued ON q (id) WHERE state = 'queued'",
        "CREATE TABLE claims (id bigint NOT NULL, claimer int NOT NULL)",
    ] {
        sqlx::query(ddl).execute(&pool).await?;
    }
    sqlx::query("INSERT INTO q (id) SELECT generate_series(1, $1)")
        .bind(jobs)
        .execute(&pool)
        .await?;
    let claim_pool = PgPoolOptions::new()
        .max_connections(claimers as u32)
        .connect(&env.database_url(db))
        .await?;

    let started = Instant::now();
    let mut tasks = Vec::new();
    for claimer in 0..claimers as i32 {
        let pool = claim_pool.clone();
        tasks.push(tokio::spawn(async move {
            while let Some(id) = claim_once(&pool, strategy).await? {
                sqlx::query("INSERT INTO claims (id, claimer) VALUES ($1, $2)")
                    .bind(id)
                    .bind(claimer)
                    .execute(&pool)
                    .await?;
            }
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    let secs = started.elapsed().as_secs_f64();

    let (total, distinct): (i64, i64) =
        sqlx::query_as("SELECT count(*), count(DISTINCT id) FROM claims")
            .fetch_one(&pool)
            .await?;
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM q WHERE state = 'queued'")
        .fetch_one(&pool)
        .await?;
    claim_pool.close().await;
    pool.close().await;
    Ok(json!({
        "secs": round3(secs),
        "jobs_per_sec": round3(distinct as f64 / secs),
        "claims_recorded": total,
        "double_claims": total - distinct,
        "jobs_left_unclaimed": left,
    }))
}

async fn claim_once(pool: &PgPool, strategy: &str) -> anyhow::Result<Option<i64>> {
    const NEXT: &str = "SELECT id FROM q WHERE state = 'queued' ORDER BY id LIMIT 1";
    const TAKE: &str = "UPDATE q SET state = 'done' WHERE id = $1";
    Ok(match strategy {
        "naive" => {
            let next: Option<i64> = sqlx::query_scalar(NEXT).fetch_optional(pool).await?;
            if let Some(id) = next {
                sqlx::query(TAKE).bind(id).execute(pool).await?;
            }
            next
        }
        "for_update" => {
            let mut tx = pool.begin().await?;
            let next: Option<i64> = sqlx::query_scalar(&format!("{NEXT} FOR UPDATE"))
                .fetch_optional(&mut *tx)
                .await?;
            if let Some(id) = next {
                sqlx::query(TAKE).bind(id).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            next
        }
        _ => {
            sqlx::query_scalar(
                "WITH next AS (SELECT id FROM q WHERE state = 'queued'
                               ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
                 UPDATE q SET state = 'done' FROM next WHERE q.id = next.id RETURNING q.id",
            )
            .fetch_optional(pool)
            .await?
        }
    })
}

/// Minimal HTTP/1.0 GET, to avoid an HTTP client dependency for one call.
async fn scrape(port: u16) -> anyhow::Result<String> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        .await?;
    let mut body = String::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_string(&mut body)).await??;
    Ok(body)
}

fn metric(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|line| {
            line.strip_prefix(name)?
                .strip_prefix(' ')?
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles() {
        let xs: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&xs, 50.0), 50.0);
        assert_eq!(percentile(&xs, 95.0), 95.0);
        assert_eq!(percentile(&xs, 100.0), 100.0);
        assert_eq!(percentile(&[7.0], 95.0), 7.0);
        assert!(percentile(&[], 50.0).is_nan());
    }

    #[test]
    fn reads_a_metric_value_from_exposition_text() {
        let text = "# HELP x\ntaskforge_claim_duration_seconds_sum 1.5\ntaskforge_claim_duration_seconds_count 3\n";
        assert_eq!(metric(text, "taskforge_claim_duration_seconds_sum"), 1.5);
        assert_eq!(metric(text, "taskforge_claim_duration_seconds_count"), 3.0);
        assert_eq!(metric(text, "missing"), 0.0);
    }
}
