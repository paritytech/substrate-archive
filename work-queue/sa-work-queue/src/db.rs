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

use crate::error::EnqueueError;
use crate::{job::Job, runner::QueueHandle};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BackgroundJob {
	/// Where this job comes from (generally the name of the job function from the proc-macro)
	pub job_type: String,
	/// Raw function data
	pub data: serde_json::Value,
}

pub async fn enqueue_job<T: Job + Send>(conn: &QueueHandle, job: T) -> Result<(), EnqueueError> {
	let job = BackgroundJob { job_type: T::JOB_TYPE.to_string(), data: serde_json::to_value(&job)? };
	conn.push(serde_json::to_vec(&job)?).await?;
	Ok(())
}

pub async fn enqueue_jobs_batch<T: Job + Send>(_conn: &QueueHandle, _jobs: Vec<T>) -> Result<(), EnqueueError> {
	println!("Not implemented yet :)");
	Ok(())
}

/// Gets jobs which failed
#[cfg(any(test, feature = "test_components"))]
pub async fn failed_job_count(conn: lapin::Connection) -> Result<i64, lapin::Error> {
	todo!()
}
