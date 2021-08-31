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

use anyhow::Result;
use assert_matches::assert_matches;
use futures::{future::FutureExt, StreamExt};
use std::thread;
use std::time::Duration;

use crate::dummy_jobs::*;
use crate::sync::Barrier;
use crate::test_guard::TestGuard;

#[test]
fn run_all_pending_jobs_returns_when_all_jobs_enqueued() -> Result<()> {
	crate::initialize();
	let barrier = Barrier::new(3);
	let runner = TestGuard::runner(barrier.clone());
	let handle = runner.handle();

	smol::block_on(async {
		barrier_job().enqueue(&handle).await.unwrap();
		barrier_job().enqueue(&handle).await.unwrap();
		runner.run_pending_tasks().unwrap();
		assert_eq!(0, runner.job_count());
	});

	barrier.wait();
	Ok(())
}

#[test]
fn wait_for_all_tasks_blocks_until_all_queued_jobs_are_finished() -> Result<()> {
	crate::initialize();
	let barrier = Barrier::new(3);
	let runner = TestGuard::runner(barrier.clone());

	let handle = runner.handle();
	smol::block_on(async {
		barrier_job().enqueue(&handle).await?;
		barrier_job().enqueue(&handle).await
	})?;
	runner.run_pending_tasks()?;
	let (tx, rx) = flume::bounded(1);
	let handle = thread::spawn(move || {
		let rx0 = rx.clone();
		let timeout = timer::Delay::new(Duration::from_millis(100));

		let res = smol::block_on(async {
			let mut stream = rx0.into_stream();
			futures::select! {
				_ = stream.next() => false,
				_ = timeout.fuse() => true
			}
		});
		assert!(res, "wait_for_jobs returned before jobs finished");

		barrier.wait();

		assert!(rx.recv().is_ok(), "wait_for_jobs didn't return");
	});

	let _ = runner.wait_for_all_tasks().unwrap();
	tx.send(())?;
	handle.join().unwrap();
	Ok(())
}

#[test]
fn panicking_jobs_are_caught_and_treated_as_failures() -> Result<()> {
	crate::initialize();
	let runner = TestGuard::dummy_runner();
	let handle = runner.handle();
	smol::block_on(async {
		panic_job().enqueue(&handle).await?;
		failure_job().enqueue(&handle).await
	})?;
	runner.run_pending_tasks()?;
	Ok(())
}

#[test]
fn run_all_pending_jobs_errs_if_jobs_dont_start_in_timeout() -> Result<()> {
	crate::initialize();
	let barrier = Barrier::new(2);
	// A runner with 1 thread where all jobs will hang indefinitely.
	// The second job will never start.
	let runner = TestGuard::builder(barrier.clone()).num_threads(1).timeout(Duration::from_millis(50)).build();

	smol::block_on(async {
		barrier_job().enqueue(&runner.handle()).await?;
		barrier_job().enqueue(&runner.handle()).await
	})?;

	let run_result = runner.run_pending_tasks();
	assert_matches!(run_result, Err(sa_work_queue::FetchError::Timeout));

	barrier.wait();
	runner.wait_for_all_tasks().unwrap();
	Ok(())
}
