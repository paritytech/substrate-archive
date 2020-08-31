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

use crate::error::Result;
use kvdb::KeyValueDB;
use kvdb_rocksdb::{Database, DatabaseConfig};
use sp_database::{ChangeRef, ColumnId, Database as DatabaseTrait, Transaction};
use std::sync::atomic::{AtomicUsize, Ordering};

pub type KeyValuePair = (Box<[u8]>, Box<[u8]>);

pub struct Config {
    /// track how many times try_catch_up_with_primary is called
    pub track_catchups: bool,
    pub config: DatabaseConfig,
}

#[derive(parity_util_mem::MallocSizeOf)]
pub struct ReadOnlyDatabase {
    inner: Database,
    catch_counter: AtomicUsize,
    track_catchups: bool,
}

impl std::fmt::Debug for ReadOnlyDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.inner.io_stats(kvdb::IoStatsKind::Overall);
        f.write_fmt(format_args!("Read Only Database Stats: {:?}", stats))
    }
}

impl ReadOnlyDatabase {
    pub fn open(config: Config, path: &str) -> Result<Self> {
        let inner = Database::open(&config.config, path)?;
        inner.try_catch_up_with_primary()?;
        Ok(Self {
            inner,
            catch_counter: AtomicUsize::new(0),
            track_catchups: config.track_catchups,
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

    pub fn iter<'a>(&'a self, col: u32) -> impl Iterator<Item = KeyValuePair> + 'a {
        self.inner.iter(col)
    }

    pub fn try_catch_up_with_primary(&self) -> Result<()> {
        if self.track_catchups {
            self.catch_counter.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.try_catch_up_with_primary()?;
        Ok(())
    }

    pub fn catch_up_count(&self) -> Option<usize> {
        if !self.track_catchups {
            log::warn!("catchup tracking is not enabled");
            None
        } else {
            Some(self.catch_counter.fetch_add(0, Ordering::Relaxed))
        }
    }
}

type DBError = std::result::Result<(), sp_database::error::DatabaseError>;
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

    fn remove(
        &self,
        _col: ColumnId,
        _key: &[u8],
    ) -> std::result::Result<(), sp_database::error::DatabaseError> {
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

/*
impl KeyValueDB for ReadOnlyDatabase {
    fn get(&self, col: u32, key: &[u8]) -> std::io::Result<Option<DBValue>> {
        self.inner.get(col, key)
    }

    fn get_by_prefix(&self, col: u32, prefix: &[u8]) -> Option<Box<[u8]>> {
        self.inner.get_by_prefix(col, prefix)
    }

    fn write(&self, _: DBTransaction) -> std::io::Result<()> {
        panic!("Can't write to a read-only database")
    }

    fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
        Box::new(self.inner.iter(col))
    }

    fn iter_with_prefix<'a>(&'a self, col: u32, prefix: &'a [u8]) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
        self.inner.iter_with_prefix(col, prefix)
    }

    fn restore(&self, new_db: &str) -> std::io::Result<()> {
        self.inner.restore(new_db)
    }

    fn io_stats(&self, kind: kvdb::IoStatsKind) -> kvdb::IoStats {
        self.io_stats(kind)
    }
}
*/
