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


mod registry;
mod job;
mod error;
mod db;
mod threadpool;
mod runner;

pub use crate::error::*;
pub use crate::job::*;

const TASK_QUEUE: &str = "TASK_QUEUE";

