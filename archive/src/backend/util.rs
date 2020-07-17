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

use crate::{
    backend::database::{ReadOnlyDatabase, Config},
    error::{ArchiveResult, Error as ArchiveError},
};
use codec::Decode;
use kvdb::DBValue;
use kvdb_rocksdb::DatabaseConfig;
// use sc_service::config::DatabaseConfig as DBConfig;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header as HeaderT, UniqueSaturatedFrom, UniqueSaturatedInto, Zero},
};
use std::convert::TryInto;
use std::path::PathBuf;

pub const NUM_COLUMNS: u32 = 11;

pub type NumberIndexKey = [u8; 4];

/// Open a database as read-only
pub fn open_database(
    path: &str,
    cache_size: usize,
    chain: &str,
    id: &str,
) -> sp_blockchain::Result<ReadOnlyDatabase> {
    let db_path = create_secondary_db_dir(chain, id);
    // need to make sure this is `Some` to open secondary instance
    let db_path = db_path.as_path().to_str().expect("Creating db path failed");
    let mut db_config = Config {
        track_catchups: false,
        config: DatabaseConfig {
            secondary: Some(db_path.to_string()),
            ..DatabaseConfig::with_columns(NUM_COLUMNS)
        }
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
    db_config.config.memory_budget = memory_budget;
    log::info!(
        target: "db",
        "Open RocksDB at {}, state column budget: {} MiB, others({}) column cache: {} MiB",
        path,
        state_col_budget,
        NUM_COLUMNS,
        other_col_budget,
    );
    super::database::ReadOnlyDatabase::open(db_config, &path)
        .map_err(|err| sp_blockchain::Error::Backend(format!("{}", err)))
}

/// Create rocksdb secondary directory if it doesn't exist yet
/// Return path to that directory
pub fn create_secondary_db_dir(chain: &str, id: &str) -> PathBuf {
    let path = if let Some(base_dirs) = dirs::BaseDirs::new() {
        let mut path = base_dirs.data_local_dir().to_path_buf();
        path.push("substrate_archive");
        path.push("rocksdb_secondary");
        path.push(chain);
        path.push(id);
        path
    } else {
        panic!("Couldn't establish substrate data local path");
    };
    std::fs::create_dir_all(path.as_path()).expect("Unable to create rocksdb secondary directory");
    path
}

#[allow(unused)]
pub(crate) mod columns {
    pub const META: u32 = 0;
    pub const STATE: u32 = 1;
    pub const STATE_META: u32 = 2;
    /// maps hashes -> lookup keys and numbers to canon hashes
    pub const KEY_LOOKUP: u32 = 3;
    pub const HEADER: u32 = 4;
    pub const BODY: u32 = 5;
    pub const JUSTIFICATION: u32 = 6;
    pub const CHANGES_TRIE: u32 = 7;
    pub const AUX: u32 = 8;
    pub const OFFCHAIN: u32 = 9;
    pub const CACHE: u32 = 10;
}

/// Keys of entries in COLUMN_META.
#[allow(unused)]
pub mod meta_keys {
    /// Type of storage (full or light).
    pub const TYPE: &[u8; 4] = b"type";
    /// Best block key.
    pub const BEST_BLOCK: &[u8; 4] = b"best";
    /// Last finalized block key.
    pub const FINALIZED_BLOCK: &[u8; 5] = b"final";
    /// Meta information prefix for list-based caches.
    pub const CACHE_META_PREFIX: &[u8; 5] = b"cache";
    /// Meta information for changes tries key.
    pub const CHANGES_TRIES_META: &[u8; 5] = b"ctrie";
    /// Genesis block hash.
    pub const GENESIS_HASH: &[u8; 3] = b"gen";
    /// Leaves prefix list key.
    pub const LEAF_PREFIX: &[u8; 4] = b"leaf";
    /// Children prefix list key.
    pub const CHILDREN_PREFIX: &[u8; 8] = b"children";
}

pub fn read_header<Block: BlockT>(
    db: &ReadOnlyDatabase,
    col_index: u32,
    col: u32,
    id: BlockId<Block>,
) -> ArchiveResult<Option<Block::Header>> {
    match read_db(db, col_index, col, id)? {
        Some(header) => match Block::Header::decode(&mut &header[..]) {
            Ok(header) => Ok(Some(header)),
            Err(_) => Err(ArchiveError::from("Error decoding header")),
        },
        None => Ok(None),
    }
}

pub fn read_db<Block>(
    db: &ReadOnlyDatabase,
    col_index: u32,
    col: u32,
    id: BlockId<Block>,
) -> ArchiveResult<Option<DBValue>>
where
    Block: BlockT,
{
    block_id_to_lookup_key(db, col_index, id).and_then(|key| match key {
        Some(key) => Ok(db.get(col, key.as_ref())),
        None => Ok(None),
    })
}

pub fn block_id_to_lookup_key<Block>(
    db: &ReadOnlyDatabase,
    key_lookup_col: u32,
    id: BlockId<Block>,
) -> Result<Option<Vec<u8>>, ArchiveError>
where
    Block: BlockT,
    sp_runtime::traits::NumberFor<Block>: UniqueSaturatedFrom<u64> + UniqueSaturatedInto<u64>,
{
    Ok(match id {
        BlockId::Number(n) => db.get(key_lookup_col, number_index_key(n)?.as_ref()),
        BlockId::Hash(h) => db.get(key_lookup_col, h.as_ref()),
    })
}

pub fn number_index_key<N: TryInto<u32>>(n: N) -> ArchiveResult<NumberIndexKey> {
    let n = n
        .try_into()
        .map_err(|_| ArchiveError::from("Block num cannot be converted to u32"))?;

    Ok([
        (n >> 24) as u8,
        ((n >> 16) & 0xff) as u8,
        ((n >> 8) & 0xff) as u8,
        (n & 0xff) as u8,
    ])
}

/// Database metadata.
#[derive(Debug)]
pub struct Meta<N, H> {
    /// Hash of the best known block.
    pub best_hash: H,
    /// Number of the best known block.
    pub best_number: N,
    /// Hash of the best finalized block.
    pub finalized_hash: H,
    /// Number of the best finalized block.
    pub finalized_number: N,
    /// Hash of the genesis block.
    pub genesis_hash: H,
}

/// Read meta from the database.
pub fn read_meta<Block>(
    db: &ReadOnlyDatabase,
    col_header: u32,
) -> Result<Meta<<<Block as BlockT>::Header as HeaderT>::Number, Block::Hash>, sp_blockchain::Error>
where
    Block: BlockT,
{
    let genesis_hash: Block::Hash = match read_genesis_hash(db)? {
        Some(genesis_hash) => genesis_hash,
        None => {
            return Ok(Meta {
                best_hash: Default::default(),
                best_number: Zero::zero(),
                finalized_hash: Default::default(),
                finalized_number: Zero::zero(),
                genesis_hash: Default::default(),
            })
        }
    };

    let load_meta_block = |desc, key| -> Result<_, sp_blockchain::Error> {
        if let Some(Some(header)) = match db.get(columns::META, key) {
            Some(id) => db
                .get(col_header, &id)
                .map(|b| Block::Header::decode(&mut &b[..]).ok()),
            None => None,
        } {
            let hash = header.hash();
            log::debug!(
                "DB Opened blockchain db, fetched {} = {:?} ({})",
                desc,
                hash,
                header.number()
            );
            Ok((hash, *header.number()))
        } else {
            Ok((genesis_hash, Zero::zero()))
        }
    };

    let (best_hash, best_number) = load_meta_block("best", meta_keys::BEST_BLOCK)?;
    let (finalized_hash, finalized_number) = load_meta_block("final", meta_keys::FINALIZED_BLOCK)?;

    Ok(Meta {
        best_hash,
        best_number,
        finalized_hash,
        finalized_number,
        genesis_hash,
    })
}

/// Read genesis hash from database.
pub fn read_genesis_hash<Hash: Decode>(
    db: &ReadOnlyDatabase,
) -> sp_blockchain::Result<Option<Hash>> {
    match db.get(columns::META, meta_keys::GENESIS_HASH) {
        Some(h) => match Decode::decode(&mut &h[..]) {
            Ok(h) => Ok(Some(h)),
            Err(err) => Err(sp_blockchain::Error::Backend(format!(
                "Error decoding genesis hash: {}",
                err
            ))),
        },
        None => Ok(None),
    }
}
