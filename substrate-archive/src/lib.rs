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

#![forbid(unsafe_code)]
// #![deny(dead_code)]

// Re-Exports
pub use sc_executor::native_executor_instance;
pub use sp_blockchain::Error as BlockchainError;
pub use sp_runtime::MultiSignature;
pub use substrate_archive_backend::{ExecutionMethod, ReadOnlyDb, RuntimeConfig, SecondaryRocksDb};

mod actors;
pub mod archive;
pub mod database;
mod error;
mod logger;
mod tasks;
mod types;
mod wasm_tracing;

pub use self::actors::{ControlConfig, System};
pub use self::archive::{Archive, ArchiveBuilder, ArchiveConfig, ChainConfig, TracingConfig};
pub use self::database::{queries, DatabaseConfig};
pub use self::error::ArchiveError;

pub mod chain_traits {
	//! Traits defining functions on the client needed for indexing
	pub use sc_client_api::client::BlockBackend;
	pub use sp_blockchain::{HeaderBackend, HeaderMetadata};
	pub use sp_runtime::traits::{BlakeTwo256, Block, IdentifyAccount, Verify};
}

/// Get the path to a local substrate directory where we can save data.
/// Platform | Value | Example
/// -- | -- | --
/// Linux | $XDG_DATA_HOME or $HOME/.local/share/substrate_archive | /home/alice/.local/share/substrate_archive/
/// macOS | $HOME/Library/Application Support/substrate_archive | /Users/Alice/Library/Application Support/substrate_archive/
/// Windows | {FOLDERID_LocalAppData}\substrate_archive | C:\Users\Alice\AppData\Local\substrate_archive
pub fn substrate_archive_default_dir() -> std::path::PathBuf {
	let base_dirs = dirs::BaseDirs::new().expect("Invalid home directory path");
	let mut path = base_dirs.data_local_dir().to_path_buf();
	path.push("substrate_archive");
	path
}

#[cfg(test)]
use test::{initialize, insert_dummy_sql, TestGuard, DATABASE_URL, PG_POOL};

#[cfg(test)]
mod test {
	use crate::database::BlockModel;
	use async_std::task;
	use once_cell::sync::Lazy;
	use sqlx::prelude::*;
	use std::sync::{Mutex, MutexGuard, Once};
	use test_common::CsvBlock;

	pub static DATABASE_URL: Lazy<String> =
		Lazy::new(|| dotenv::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set to run tests!"));

	pub const DUMMY_HASH: [u8; 2] = [0x13, 0x37];

	impl From<test_common::CsvBlock> for BlockModel {
		fn from(csv: CsvBlock) -> BlockModel {
			BlockModel {
				id: csv.id,
				parent_hash: csv.parent_hash,
				hash: csv.hash,
				block_num: csv.block_num,
				state_root: csv.state_root,
				extrinsics_root: csv.extrinsics_root,
				digest: csv.digest,
				ext: csv.ext,
				spec: csv.spec,
			}
		}
	}

	pub static PG_POOL: Lazy<sqlx::PgPool> = Lazy::new(|| {
		task::block_on(async {
			let pool = sqlx::postgres::PgPoolOptions::new()
				.min_connections(4)
				.max_connections(8)
				.idle_timeout(std::time::Duration::from_millis(3600))
				.connect(&DATABASE_URL)
				.await
				.expect("Couldn't initialize postgres pool for tests");
			pool
		})
	});

	static INIT: Once = Once::new();
	pub fn initialize() {
		INIT.call_once(|| {
			pretty_env_logger::init();
			let url: &str = &DATABASE_URL;
			task::block_on(async {
				crate::database::migrate(url).await.unwrap();
			});
		});
	}

	static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

	pub fn insert_dummy_sql() {
		task::block_on(async {
			sqlx::query(
				r#"
                    INSERT INTO metadata (version, meta)
                    VALUES($1, $2)
                "#,
			)
			.bind(0)
			.bind(&DUMMY_HASH[0..2])
			.execute(&*PG_POOL)
			.await
			.unwrap();

			// insert a dummy block
			sqlx::query(
                    "
                        INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec)
                        VALUES($1, $2, $3, $4, $5, $6, $7, $8)
                    ")
                    .bind(&DUMMY_HASH[0..2])
                    .bind(&DUMMY_HASH[0..2])
                    .bind(0)
                    .bind(&DUMMY_HASH[0..2])
                    .bind(&DUMMY_HASH[0..2])
                    .bind(&DUMMY_HASH[0..2])
                    .bind(&DUMMY_HASH[0..2])
                    .bind(0)
                    .execute(&*PG_POOL)
                    .await
                    .expect("INSERT");
		});
	}

	pub struct TestGuard<'a>(MutexGuard<'a, ()>);
	impl<'a> TestGuard<'a> {
		pub(crate) fn lock() -> Self {
			let guard = TestGuard(TEST_MUTEX.lock().expect("Test mutex panicked"));
			guard
		}
	}

	impl<'a> Drop for TestGuard<'a> {
		fn drop(&mut self) {
			task::block_on(async move {
				let mut conn = crate::PG_POOL.acquire().await.unwrap();
				conn.execute(
					"
					TRUNCATE TABLE metadata CASCADE;
					TRUNCATE TABLE storage CASCADE;
					TRUNCATE TABLE blocks CASCADE;
					TRUNCATE TABLE state_traces CASCADE;
					TRUNCATE TABLE _background_tasks;
					",
				)
				.await
				.unwrap();
			});
		}
	}
}
