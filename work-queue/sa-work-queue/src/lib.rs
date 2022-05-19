//! A library that utilizes a RabbitMQ queue to execute background tasks
//!
//!### Usage
//!
//! The core of this work-queue is the `#[background_job]` macro which implements
//! the [`Job`] trait, allowing for the enqueuing & performing of jobs.
//!
//! ```no_run
//! #[sa_work_queue::background_job]
//! fn do_some_blocking_work(name: String) -> Result<(), sa_work_queue::PerformError> {
//!    std::thread::sleep(std::time::Duration::from_millis(150));
//!    println!("Job {} finished!", name);
//!    Ok(())
//! }
//! let runner = sa_work_queue::Runner::builder((), "Im a job")
//!    .register_job::<do_some_blocking_work::Job>()
//!    .build()
//!    .unwrap();
//! runner.run_pending_tasks().unwrap();
//! ```
//!
//! Each background job may have a `Environment` attached that is passed into every job.
//! This could be useful to store dynamic information such as database urls, database connections, backend clients, etc.
//!
//!
//!```no_run
//!
//!pub struct MyEnvironment {
//!    db_url: String
//!}
//!#[sa_work_queue::background_job]
//!fn do_some_blocking_work(env: &MyEnvironment, name: String) -> Result<(), sa_work_queue::PerformError> {
//!   std::thread::sleep(std::time::Duration::from_millis(150));
//!   println!("Job {} finished with database url {}!", name, env.db_url);
//!   Ok(())
//!}
//!
//!let env = MyEnvironment {
//!   db_url: "postgres://postgres@localhost:5432/postgres".to_string()
//!};
//!
//!let runner = sa_work_queue::Runner::builder(env, "Im a job")
//!   .register_job::<do_some_blocking_work::Job>()
//!   .build()
//!   .unwrap();
//!runner.run_pending_tasks().unwrap();
//!```
//!
//! ### RabbitMQ Requirements
//!
//! This library requires a RabbitMQ service running on the host. The address is defined by [`Builder::new`].
//! The URL along with RabbitMQ prefetch and queue name may be configured.
//! function
//! ```no_run
//!sa_work_queue::Runner::builder((), "amqp://localhost:5672")
//!    .queue_name("my-work-queue")
//!    .num_threads(2)
//!    .prefetch(200)
//!    .timeout(std::time::Duration::from_secs(5))
//!    .build()
//!    .unwrap();
//! ```
//!
//!

#![deny(unused, dead_code)]
#![forbid(unsafe_code)]
#[doc(hidden)]
pub extern crate async_trait;
#[doc(hidden)]
pub extern crate inventory;
#[doc(hidden)]
pub extern crate serde;
#[doc(hidden)]
pub use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[doc(hidden)]
pub use registry::JobVTable;

mod error;
mod job;
mod registry;
mod runner;
mod threadpool;

pub use crate::error::*;
pub use crate::job::*;
pub use runner::{Builder, Event, QueueHandle, Runner};
pub use sa_work_queue_proc_macro::*;

#[cfg(test)]
pub fn initialize() {
	let _ = pretty_env_logger::try_init();
}
