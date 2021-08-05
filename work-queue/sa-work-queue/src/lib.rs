// #![forbid(unsafe_code)]
// #![deny(dead_code)]

#[allow(warnings, unused)]
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

mod db;
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
	pretty_env_logger::try_init().unwrap()
}
