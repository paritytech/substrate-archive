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

use crate::sync::Barrier;
pub use sa_work_queue::Job;
use sa_work_queue::PerformError;

#[sa_work_queue::background_job]
pub fn barrier_job(env: &Barrier) -> Result<(), PerformError> {
    env.wait();
    Ok(())
}

#[sa_work_queue::background_job]
pub fn failure_job() -> Result<(), PerformError> {
    Err(PerformError::from("fail on purpose".to_string()))
}

#[sa_work_queue::background_job]
pub fn panic_job() -> Result<(), PerformError> {
    panic!()
}
