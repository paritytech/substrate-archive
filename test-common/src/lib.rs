// Copyright 2018-2021 Parity Technologies (UK) Ltd.
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
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

mod database;
mod queue;

use std::{sync::{Mutex, MutexGuard}, fs::File, path::PathBuf};

use anyhow::Error;
use once_cell::sync::Lazy;
use async_std::task;
use sqlx::prelude::*;

pub use test_wasm::wasm_binary_unwrap;
pub use database::*;
pub use queue::*;

static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
pub struct TestGuard<'a>(MutexGuard<'a, ()>);
impl<'a> TestGuard<'a> {
    pub fn lock() -> Self {
        let guard = TestGuard(TEST_MUTEX.lock().expect("Test mutex panicked"));
        guard
    }
}

impl<'a> Drop for TestGuard<'a> {
    fn drop(&mut self) {
        task::block_on(async move {
            let mut conn = database::PG_POOL.acquire().await.unwrap();
            conn.execute(
                "
                TRUNCATE TABLE metadata CASCADE;
                TRUNCATE TABLE storage CASCADE;
                TRUNCATE TABLE blocks CASCADE;
                TRUNCATE TABLE state_traces CASCADE;
                ",
            )
            .await
            .unwrap();
            let channel = queue::AMQP_CONN.create_channel().wait().unwrap();
            channel.queue_delete(queue::TASK_QUEUE, Default::default()).wait().unwrap();
        });
    }
}

pub struct CsvBlock {
	pub id: i32,
	pub parent_hash: Vec<u8>,
	pub hash: Vec<u8>,
	pub block_num: i32,
	pub state_root: Vec<u8>,
	pub extrinsics_root: Vec<u8>,
	pub digest: Vec<u8>,
	pub ext: Vec<u8>,
	pub spec: i32,
}

/// Get 10,000 blocks from kusama, starting at block 3,000,000
pub fn get_kusama_blocks() -> Result<Vec<CsvBlock>, Error> {
	let mut dir = PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
	dir.extend(["test-data", "10K_ksm_blocks.csv"].iter());
	let blocks = File::open(dir)?;
	let mut rdr = csv::ReaderBuilder::new().has_headers(false).delimiter(b'\t').from_reader(blocks);
	let mut blocks = Vec::new();
	for line in rdr.records() {
		let line = line?;
		let id: i32 = line.get(0).unwrap().parse()?;
		let parent_hash: Vec<u8> = hex::decode(line.get(1).unwrap().strip_prefix("\\\\x").unwrap())?;
		let hash: Vec<u8> = hex::decode(line.get(2).unwrap().strip_prefix("\\\\x").unwrap())?;
		let block_num: i32 = line.get(3).unwrap().parse()?;
		let state_root: Vec<u8> = hex::decode(line.get(4).unwrap().strip_prefix("\\\\x").unwrap())?;
		let extrinsics_root: Vec<u8> = hex::decode(line.get(5).unwrap().strip_prefix("\\\\x").unwrap())?;
		let digest = hex::decode(line.get(6).unwrap().strip_prefix("\\\\x").unwrap())?;
		let ext: Vec<u8> = hex::decode(line.get(7).unwrap().strip_prefix("\\\\x").unwrap())?;
		let spec: i32 = line.get(8).unwrap().parse()?;
		let block = CsvBlock { id, parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec };
		blocks.push(block);
	}
	Ok(blocks)
}
