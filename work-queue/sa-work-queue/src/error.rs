// Copyright 2018-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive. If not, see <http://www.gnu.org/licenses/>.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
	/// Error Enqueing a task for execution later
	#[error(transparent)]
	Enqueue(#[from] EnqueueError),
	/// Error performing a task
	#[error(transparent)]
	Perform(#[from] PerformError),
	/// Error Fetching a task for execution on a threadpool/executor
	#[error(transparent)]
	Fetch(#[from] FetchError),
	/// Error executing SQL
	#[error(transparent)]
	Mq(#[from] lapin::Error),
	#[error(transparent)]
	Env(#[from] std::env::VarError),
	#[error(transparent)]
	Json(#[from] serde_json::Error),
	#[error("{0}")]
	Msg(String),
}

#[derive(Debug, Error)]
pub enum FetchError {
	#[error("Got no response from worker")]
	NoMessage,
	#[error("Timeout reached while waiting for worker to finish")]
	Timeout,
	#[error("Couldn't load job from storage {0}")]
	FailedLoadingJob(#[from] lapin::Error),
	#[error("Failed to decode job {0}")]
	FailedDecode(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum EnqueueError {
	/// An error occurred while trying to insert the task into Postgres
	#[error("Error inserting task {0}")]
	Sql(#[from] lapin::Error),
	/// Error encoding job arguments
	#[error("Error encoding task for insertion {0}")]
	Encode(#[from] serde_json::Error),
	#[error("Error enqueuing batch tasks")]
	Batch(#[from] BatchInsertError),
}

#[derive(Debug, Error)]
pub enum BatchInsertError {
	#[error("Error converting between integer and ascii")]
	Itoa(#[from] std::fmt::Error),
	#[error("Error inserting task {0}")]
	Sql(#[from] lapin::Error),
}

/// Catch-all error for jobs
pub type PerformError = Box<dyn std::error::Error + Send + Sync>;

#[doc(hidden)]
#[cfg(any(test, feature = "test_components"))]
#[derive(Debug, PartialEq, Eq)]
pub enum FailedJobsError {
	/// Jobs that failed to run
	JobsFailed(
		/// Number of failed jobs
		i64,
	),
}

impl From<String> for Error {
	fn from(err: String) -> Error {
		Error::Msg(err)
	}
}
