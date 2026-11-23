//! Worker runtime for TaskForge.
//!
//! A worker claims jobs from the store (up to its concurrency limit), runs
//! the handler registered for each job's kind, and records the outcome. It only ever runs
//! code that was registered at start-up: a job is a *name* plus JSON data,
//! never code or a command line.

pub mod handlers;
mod registry;
mod time;
mod worker;

pub use registry::{HandlerError, HandlerFuture, HandlerRegistry, JobContext, JobHandler};
pub use time::{LocalTime, TokioTime};
pub use worker::{Outcome, Worker, WorkerConfig};
