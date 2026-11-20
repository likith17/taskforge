//! Shared helpers for the worker integration tests.
#![allow(dead_code)] // each test binary uses a different subset

use std::future::Future;
use std::time::Duration;

use serde_json::Value;
use taskforge_domain::{Job, JobId, JobState, JobSubmission, NewJob, RetryPolicy};
use taskforge_storage::JobStore;
use taskforge_worker::{HandlerRegistry, Worker, WorkerConfig};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub fn config(worker_id: &str) -> WorkerConfig {
    WorkerConfig {
        worker_id: worker_id.to_owned(),
        queues: vec!["default".to_owned()],
        poll_interval: Duration::from_millis(20),
        lease: Duration::from_secs(60),
        concurrency: 4,
        shutdown_grace: Duration::from_secs(5),
    }
}

pub fn worker_with(store: &JobStore, registry: HandlerRegistry) -> Worker {
    Worker::new(store.clone(), registry, config("test-worker"))
}

/// Retries almost immediately, so tests do not wait out real backoff.
pub fn fast_retries(store: JobStore) -> JobStore {
    store.with_retry_policy(RetryPolicy::new(
        Duration::from_millis(20),
        Duration::from_millis(40),
    ))
}

pub async fn submit(store: &JobStore, kind: &str, payload: Value) -> Job {
    submit_with(store, kind, payload, |_| {}).await
}

pub async fn submit_with(
    store: &JobStore,
    kind: &str,
    payload: Value,
    edit: impl FnOnce(&mut JobSubmission),
) -> Job {
    let mut submission = JobSubmission {
        kind: kind.to_owned(),
        payload: Some(payload),
        ..Default::default()
    };
    edit(&mut submission);
    store
        .insert(&NewJob::try_from(submission).unwrap())
        .await
        .unwrap()
}

pub async fn fetch(store: &JobStore, id: JobId) -> Job {
    store.get(id).await.unwrap().unwrap()
}

/// Poll until `condition` holds, failing the test after `timeout`.
pub async fn eventually<F, Fut>(what: &str, timeout: Duration, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let waited = tokio::time::timeout(timeout, async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await;
    assert!(
        waited.is_ok(),
        "timed out after {timeout:?} waiting for: {what}"
    );
}

/// Wait for a job to reach `state` and return it.
pub async fn wait_for_state(store: &JobStore, id: JobId, state: JobState) -> Job {
    eventually(
        &format!("job {id} to be {state}"),
        Duration::from_secs(15),
        || async { fetch(store, id).await.state == state },
    )
    .await;
    fetch(store, id).await
}

/// A worker running in the background, with the two ways it can end.
pub struct Running {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

pub fn start(worker: Worker) -> Running {
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(async move { worker.run(stopped).await });
    Running { stop, task }
}

impl Running {
    /// Graceful: what SIGTERM does.
    pub async fn shutdown(self) {
        self.stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(20), self.task)
            .await
            .expect("worker should stop within its grace period")
            .unwrap();
    }

    /// Abrupt: what `kill -9` does. Nothing is reported or released; from the
    /// database's point of view the worker simply falls silent.
    pub async fn crash(self) {
        self.task.abort();
        let _ = self.task.await;
        // Let the aborted handler tasks unwind.
        tokio::task::yield_now().await;
    }
}
