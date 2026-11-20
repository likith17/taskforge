use std::time::Duration;

use sqlx::PgPool;
use taskforge_domain::{JobState, JobSubmission, NewJob};
use taskforge_scheduler::{Reaper, ReaperConfig};
use taskforge_storage::{JobStore, PgJobStore};
use tokio::sync::watch;

fn queues() -> Vec<String> {
    vec!["default".to_owned()]
}

async fn enqueue(store: &PgJobStore, n: usize) {
    for _ in 0..n {
        let job = NewJob::try_from(JobSubmission {
            kind: "echo".to_owned(),
            ..Default::default()
        })
        .unwrap();
        store.insert(&job).await.unwrap();
    }
}

async fn count(pool: &PgPool, state: JobState) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM jobs WHERE state = $1")
        .bind(state.as_str())
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn tick_recovers_every_expired_lease_across_batches(pool: PgPool) {
    let store = PgJobStore::new(pool.clone());
    enqueue(&store, 25).await;
    // Twenty abandoned by a worker with a lease that is already over; five
    // held by a healthy one.
    let dead_lease = Duration::from_millis(1);
    assert_eq!(
        store
            .claim("dead", &queues(), dead_lease, 20)
            .await
            .unwrap()
            .len(),
        20
    );
    assert_eq!(
        store
            .claim("alive", &queues(), Duration::from_secs(60), 5)
            .await
            .unwrap()
            .len(),
        5
    );
    tokio::time::sleep(Duration::from_millis(20)).await;

    // A batch size smaller than the backlog: one tick must still clear it all.
    let reaper = Reaper::new(
        store,
        ReaperConfig {
            interval: Duration::from_secs(1),
            batch: 7,
            ..ReaperConfig::default()
        },
    );
    let reaped = reaper.tick().await.unwrap();
    assert_eq!((reaped.requeued, reaped.dead, reaped.cancelled), (20, 0, 0));

    assert_eq!(count(&pool, JobState::Queued).await, 20);
    assert_eq!(
        count(&pool, JobState::Running).await,
        5,
        "live leases are left alone"
    );
    assert_eq!(reaper.tick().await.unwrap().total(), 0);
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn run_loop_recovers_leases_as_they_expire_and_stops_on_shutdown(pool: PgPool) {
    let store = PgJobStore::new(pool.clone());
    enqueue(&store, 3).await;
    store
        .claim("dead", &queues(), Duration::from_millis(150), 3)
        .await
        .unwrap();

    let reaper = Reaper::new(
        store,
        ReaperConfig {
            interval: Duration::from_millis(25),
            batch: 100,
            ..ReaperConfig::default()
        },
    );
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(async move { reaper.run(stopped).await });

    // Not yet expired: nothing happens.
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(count(&pool, JobState::Running).await, 3);

    tokio::time::timeout(Duration::from_secs(5), async {
        while count(&pool, JobState::Queued).await != 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("expired leases should be recovered");

    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("reaper should stop promptly")
        .unwrap();
}

#[sqlx::test(migrator = "taskforge_storage::MIGRATOR")]
async fn run_loop_survives_a_database_outage(pool: PgPool) {
    let reaper = Reaper::new(
        PgJobStore::new(pool.clone()),
        ReaperConfig {
            interval: Duration::from_millis(20),
            batch: 100,
            ..ReaperConfig::default()
        },
    );
    pool.close().await;
    assert!(reaper.tick().await.unwrap_err().is_unavailable());

    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(async move { reaper.run(stopped).await });
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(!running.is_finished(), "keeps trying instead of exiting");
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("reaper should stop promptly")
        .unwrap();
}
