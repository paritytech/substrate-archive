// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

//! various utilities that make interfacing with substrate easier

use kvdb_rocksdb::DatabaseConfig;
use sp_database::Database as DatabaseTrait;
use sp_runtime::traits::Block as BlockT;
use std::sync::Arc;
use tempdir::TempDir;

pub const NUM_COLUMNS: u32 = 11;

// taken from substrate/client/db/src/lib.rs
const DB_HASH_LEN: usize = 32;
pub type DbHash = [u8; DB_HASH_LEN];

/// Open a database as read-only
pub fn open_database<Block: BlockT>(
    path: &str,
    cache_size: usize,
) -> sp_blockchain::Result<Arc<dyn DatabaseTrait<DbHash>>> {
    let mut db_config = DatabaseConfig {
        secondary: TempDir::new("substrate-archive")
            .expect("Could not instantiate secondary database directory")
            .path()
            .to_str()
            .map(|s| s.to_string()),
        ..DatabaseConfig::with_columns(NUM_COLUMNS)
    };
    let state_col_budget = (cache_size as f64 * 0.9) as usize;
    let other_col_budget = (cache_size - state_col_budget) / (NUM_COLUMNS as usize - 1);
    let mut memory_budget = std::collections::HashMap::new();

    for i in 0..NUM_COLUMNS {
        if i == 1 {
            memory_budget.insert(i, state_col_budget);
        } else {
            memory_budget.insert(i, other_col_budget);
        }
    }
    db_config.memory_budget = memory_budget;
    log::info!(
        target: "db",
        "Open RocksDB database at {}, state column budget: {} MiB, others({}) column cache: {} MiB",
        path,
        state_col_budget,
        NUM_COLUMNS,
        other_col_budget,
    );
    let db = super::database::ReadOnlyDatabase::open(&db_config, &path)
        .map_err(|err| sp_blockchain::Error::Backend(format!("{}", err)))?;
    Ok(sp_database::as_database(db))
}
