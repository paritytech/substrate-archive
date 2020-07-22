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

//! Custom Read-Only Database Instance using RocksDB Secondary features
//! Will try catching up with primary database on every `get()`

use crate::error::{ArchiveResult, Error};
use codec::Decode;
use kvdb::KeyValueDB as _;
use kvdb_rocksdb::{Database, DatabaseConfig};
use sp_database::{ChangeRef, ColumnId, Database as DatabaseTrait, Transaction};
use sp_runtime::traits::Block as BlockT;
use std::{
    convert::TryInto,
    sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
}};

pub struct Config {
    /// track how many times try_catch_up_with_primary is called
    pub track_catchups: bool,
    pub config: DatabaseConfig,
}

pub struct ReadOnlyDatabase {
    inner: Database,
    catch_counter: AtomicUsize,
    config: Config,
}

impl std::fmt::Debug for ReadOnlyDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.inner.io_stats(kvdb::IoStatsKind::Overall);
        f.write_fmt(format_args!("Read Only Database Stats: {:?}", stats))
    }
}

impl ReadOnlyDatabase {
    pub fn open(config: Config, path: &str) -> ArchiveResult<Self> {
        let inner = Database::open(&config.config, path)?;
        inner.try_catch_up_with_primary()?;
        Ok(Self {
            inner,
            catch_counter: AtomicUsize::new(0),
            config,
        })
    }

    pub fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
        match self.inner.get(col, key) {
            Ok(v) => v,
            Err(e) => {
                log::debug!(
                    "{}, Catching up with primary and trying again...",
                    e.to_string()
                );
                self.try_catch_up_with_primary().ok()?;
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

    pub fn iter_headers<Block: BlockT, F>(
        self: Arc<Self>,
        fun: F,
    ) 
    where
        F: Fn(Option<Block::Header>)
    {
        let db = self.clone();
        self
            .inner
            .iter(super::util::columns::KEY_LOOKUP)
            .map(move |(key, value)| {
                println!("KEY: {:#X?}", key);
                db.get(super::util::columns::HEADER, value.as_ref())
            })
            .map(|h| match h {
                Some(header) => match Block::Header::decode(&mut &header[..]) {
                    Ok(header) => Some(header),
                    Err(e) => {
                        log::error!("{}", e.to_string());
                        None
                    }
                },
                None => None
            }).for_each(|h| {
                fun(h)
            });
    }

    pub fn iter_from_headers<Block: BlockT, F>(
        self: Arc<Self>,
        block_num: u32,
        fun: F,
    ) 
    where
        F: Fn(Option<Block::Header>)
    {
        let db = self.clone();
        let key = super::util::number_index_key(block_num).unwrap();
        self
            .inner
            .iter_with_prefix(super::util::columns::KEY_LOOKUP, &key)
            .filter(|(key, value)| {
                // println!("Key length: {}", key.len());
                // println!("Value (Header Query Key): {:#?}", value);
                let arr: &[u8; 4] = key[0..4].try_into().unwrap();
                let num = u32::from_be_bytes(*arr);
                // println!("NUM: {}", num);
                key.len() == 4 
            })
            .map(move |(key, value)| {
                println!("KEY: {:#X?}", key);
                db.get(super::util::columns::HEADER, value.as_ref())
            })
            .map(|h| match h {
                Some(header) => match Block::Header::decode(&mut &header[..]) {
                    Ok(header) => Some(header),
                    Err(e) => {
                        log::error!("{}", e.to_string());
                        None
                    }
                },
                None => None
            }).for_each(|h| {
                fun(h)
            });
    }

    pub fn try_catch_up_with_primary(&self) -> ArchiveResult<()> {
        if self.config.track_catchups {
            self.catch_counter.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.try_catch_up_with_primary()?;
        Ok(())
    }

    pub fn catch_up_count(&self) -> Option<usize> {
        if !self.config.track_catchups {
            log::warn!("catchup tracking is not enabled");
            None
        } else {
            Some(self.catch_counter.fetch_add(0, Ordering::Relaxed))
        }
    }
}

type DBError = Result<(), sp_database::error::DatabaseError>;
//TODO: Remove panics with a warning that database has not been written to / is read-only
/// Preliminary trait for ReadOnlyDatabase
impl<H: Clone> DatabaseTrait<H> for ReadOnlyDatabase {
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

    fn remove(&self, _col: ColumnId, _key: &[u8]) -> Result<(), sp_database::error::DatabaseError> {
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
