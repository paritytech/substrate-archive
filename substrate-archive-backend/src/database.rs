// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

//! Custom Read-Only Database Instance using RocksDB Secondary features
//! Will try catching up with primary database on every `get()`

use std::{collections::HashMap, fmt, io, path::PathBuf};

use kvdb::KeyValueDB;
use kvdb_rocksdb::{Database, DatabaseConfig};

use sp_database::{ChangeRef, ColumnId, Database as DatabaseTrait, Transaction};

const NUM_COLUMNS: u32 = 11;

pub type KeyValuePair = (Box<[u8]>, Box<[u8]>);

// Archive specific K/V database reader implementation
pub trait ReadOnlyDB: Send + Sync {
	/// Read key/value pairs from the database
	fn get(&self, col: u32, key: &[u8]) -> Option<Vec<u8>>;
	/// Iterate over all blocks in the database
	fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a>;
	/// Catch up with the latest information added to the database
	fn catch_up_with_primary(&self) -> io::Result<()>;
	// Open database as read-only
	fn open_database(path: &str, cache_size: usize, db_path: PathBuf) -> io::Result<Self>
	where
		Self: Sized;
}

pub struct Config {
	pub config: DatabaseConfig,
}

#[derive(parity_util_mem::MallocSizeOf)]
pub struct SecondaryRocksDB {
	inner: Database,
}

impl fmt::Debug for SecondaryRocksDB {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let stats = self.inner.io_stats(kvdb::IoStatsKind::Overall);
		f.write_fmt(format_args!("Read Only Database Stats: {:?}", stats))
	}
}

impl SecondaryRocksDB {
	pub fn open(config: Config, path: &str) -> io::Result<Self> {
		let inner = Database::open(&config.config, path)?;
		inner.try_catch_up_with_primary()?;
		Ok(Self { inner })
	}

	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		match self.inner.get(col, key) {
			Ok(v) => v,
			Err(e) => {
				log::debug!("{}, Catching up with primary and trying again...", e.to_string());
				self.catch_up_with_primary().ok()?;
				match self.inner.get(col, key) {
					Ok(v) => v,
					Err(e) => {
						log::error!("{}", e.to_string());
						None
					}
				}
			}
		}
	}
}

impl ReadOnlyDB for SecondaryRocksDB {
	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		self.get(col, key)
	}

	fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
		Box::new(self.inner.iter(col))
	}

	fn catch_up_with_primary(&self) -> io::Result<()> {
		self.inner.try_catch_up_with_primary()
	}

	fn open_database(path: &str, cache_size: usize, db_path: PathBuf) -> io::Result<SecondaryRocksDB> {
		// need to make sure this is `Some` to open secondary instance
		let db_path = db_path.as_path().to_str().expect("Creating db path failed");
		let mut db_config = Config {
			config: DatabaseConfig {
				secondary: Some(db_path.to_string()),
				..DatabaseConfig::with_columns(NUM_COLUMNS)
			},
		};
		let state_col_budget = (cache_size as f64 * 0.9) as usize;
		let other_col_budget = (cache_size - state_col_budget) / (NUM_COLUMNS as usize - 1);
		let mut memory_budget = HashMap::new();

		for i in 0..NUM_COLUMNS {
			if i == 1 {
				memory_budget.insert(i, state_col_budget);
			} else {
				memory_budget.insert(i, other_col_budget);
			}
		}
		db_config.config.memory_budget = memory_budget;
		log::info!(
			target: "db",
			"Open RocksDB at {}, state column budget: {} MiB, others({}) column cache: {} MiB",
			path,
			state_col_budget,
			NUM_COLUMNS,
			other_col_budget,
		);
		Self::open(db_config, &path)
	}
}

type DBError = std::result::Result<(), sp_database::error::DatabaseError>;
//TODO: Remove panics with a warning that database has not been written to / is read-only
/// Preliminary trait for ReadOnlyDB
impl<H: Clone> DatabaseTrait<H> for SecondaryRocksDB {
	fn commit(&self, _transaction: Transaction<H>) -> DBError {
		panic!("Read only db")
	}

	fn commit_ref<'a>(&self, _transaction: &mut dyn Iterator<Item = ChangeRef<'a, H>>) -> DBError {
		panic!("Read only db")
	}

	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		self.get(col, key)
	}
	// with_get -> default is fine

	fn remove(&self, _col: ColumnId, _key: &[u8]) -> std::result::Result<(), sp_database::error::DatabaseError> {
		panic!("Read only db")
	}

	fn lookup(&self, _hash: &H) -> Option<Vec<u8>> {
		unimplemented!();
	}

	// with_lookup -> default
	/*
		fn store(&self, _hash: , _preimage: _) {
			panic!("Read only db")
		}
	*/
}
