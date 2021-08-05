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
// along with coil.  If not, see <http://www.gnu.org/licenses/>.

use crate::{
	error::{EnqueueError, PerformError},
	runner::QueueHandle,
};
use serde::{de::DeserializeOwned, Serialize};

/// Background job
#[async_trait::async_trait]
pub trait Job: Serialize + DeserializeOwned {
	///  The environment this job is run with.
	///  This is a struct you define,
	///  which should encapsulate things like database connection pools,
	///  any configuration, and any other static data or shared resources.
	type Environment: 'static + Send + Sync;

	/// The key to use for storing this job.
	/// Typically this is the name of your struct in `snake_case`.
	const JOB_TYPE: &'static str;

	#[doc(hidden)]
	/// inserts the job into the Postgres Database
	async fn enqueue(self, conn: &QueueHandle) -> Result<(), EnqueueError> {
		crate::db::enqueue_job(conn, self).await
	}

	/// Logic for running a synchronous job
	#[doc(hidden)]
	fn perform(self, _: &Self::Environment) -> Result<(), PerformError> {
		panic!("Running Sync job when it should be async!");
	}
}

#[async_trait::async_trait]
pub trait JobExt: Job {
	async fn enqueue_batch(data: Vec<Self>, conn: &QueueHandle) -> Result<(), EnqueueError> {
		todo!()
	}
}

impl<T> JobExt for T where T: Job {}
