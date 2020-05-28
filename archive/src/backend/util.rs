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

use super::database::ReadOnlyDatabase;
use kvdb_rocksdb::DatabaseConfig;
use codec::Decode;
// Don't want sc_service
use sc_service::config::DatabaseConfig as DBConfig;
use sp_database::Database as DatabaseTrait;
use sp_runtime::{traits::{Block as BlockT, UniqueSaturatedFrom, UniqueSaturatedInto}, generic::BlockId};
use sp_trie::DBValue;
use std::{path::Path, sync::Arc, convert::TryInto};

pub const NUM_COLUMNS: u32 = 11;

// taken from substrate/client/db/src/lib.rs
const DB_HASH_LEN: usize = 32;
pub type DbHash = [u8; DB_HASH_LEN];
pub type NumberIndexKey = [u8; 4];

/// Open a rocksdb Database as Read-Only
pub fn open_database<Block: BlockT>(
    path: &Path,
    cache_size: usize,
    chain: &str,
    id: &str,
) -> sp_blockchain::Result<DBConfig> {
    let path = path.to_str().expect("Path to rocksdb not valid UTF-8");
    Ok(DBConfig::Custom(open_db::<Block>(
        path, cache_size, chain, id,
    )?))
}

/// Open a database as read-only
fn open_db<Block: BlockT>(
    path: &str,
    cache_size: usize,
    chain: &str,
    id: &str,
) -> sp_blockchain::Result<Arc<dyn DatabaseTrait<DbHash>>> {
    let db_path = crate::util::create_secondary_db_dir(chain, id);
    // need to make sure this is `Some` to open secondary instance
    let db_path = db_path.as_path().to_str().expect("Creating db path failed");
    let mut db_config = DatabaseConfig {
        secondary: Some(db_path.to_string()),
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
        "Open RocksDB at {}, state column budget: {} MiB, others({}) column cache: {} MiB",
        path,
        state_col_budget,
        NUM_COLUMNS,
        other_col_budget,
    );
    let db = ReadOnlyDatabase::open(&db_config, &path)
        .map_err(|err| sp_blockchain::Error::Backend(format!("{}", err)))?;
    Ok(sp_database::as_database(db))
}


pub fn read_header<Block: BlockT>(
    db: &ReadOnlyDatabase, 
    col_index: u32,
    col: u32,
    id: BlockId<Block>
) -> sp_blockchain::Result<Option<Block::Header>> {
    match read_db(db, col_index, col, id)? {
        Some(header) => match Block::Header::decode(&mut &header[..]) {
            Ok(header) => Ok(Some(header)),
            Err(_) => return Err(
                sp_blockchain::Error::Backend("Error decoding header".into())
            ),
        }
        None => Ok(None)
    }
}

pub fn read_db<Block>(
    db: &ReadOnlyDatabase,
    col_index: u32,
    col: u32,
    id: BlockId<Block>
) -> sp_blockchain::Result<Option<DBValue>>
where 
    Block: BlockT
{
    block_id_to_lookup_key(db, col_index, id).and_then(|key| match key {
        Some(key) => Ok(db.get(col, key.as_ref())),
        None => Ok(None)
    })
}

pub fn block_id_to_lookup_key<Block>(
    db: &ReadOnlyDatabase,
    key_lookup_col: u32,
    id: BlockId<Block>
) -> Result<Option<Vec<u8>>, sp_blockchain::Error> 
where
    Block: BlockT,
    sp_runtime::traits::NumberFor<Block>: UniqueSaturatedFrom<u64> + UniqueSaturatedInto<u64>,
{
    Ok(match id {
            BlockId::Number(n) => db.get(key_lookup_col, number_index_key(n)?.as_ref()),
            BlockId::Hash(h) => db.get(key_lookup_col, h.as_ref())
    })
}

pub fn number_index_key<N: TryInto<u32>>(n: N) -> sp_blockchain::Result<NumberIndexKey> {
    let n = n.try_into().map_err(|_| {
        sp_blockchain::Error::Backend("Block num cannot be converted to u32".into())
    })?;

    Ok([
        (n >> 24) as u8,
        ((n >> 16) & 0xff) as u8,
        ((n >> 8) & 0xff) as u8,
        (n & 0xff) as u8
    ])
}