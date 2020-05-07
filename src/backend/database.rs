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

use sp_database::{Database as DatabaseTrait, ColumnId, Transaction, ChangeRef};
use kvdb_rocksdb::{Database, DatabaseConfig};
use kvdb::{KeyValueDB, DBValue, DBTransaction};
use parity_util_mem::MallocSizeOf;
use std::io;

pub type KeyValuePair = (Box<[u8]>, Box<[u8]>);

#[derive(MallocSizeOf)]
pub struct ReadOnlyDatabase {
    inner: Database
}

impl ReadOnlyDatabase {
    pub fn open(config: &DatabaseConfig, path: &str) -> io::Result<Self> {
        let inner = Database::open(config, path)?;
        Ok(Self { inner })
    }
}

//TODO: Remove panics with a warning that database has not been written to / is read-only
/// Preliminary trait for ReadOnlyDatabase
impl<H: Clone> DatabaseTrait<H> for ReadOnlyDatabase {
    fn commit(&self, transaction: Transaction<H> ) {
        panic!("Read only db")
    }

    fn commit_ref<'a>(&self, transaction: &mut dyn Iterator<Item = ChangeRef<'a, H>>) {
        panic!("Read only db")
    }

    fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.try_catch_up_with_primary().ok();
        // TODO: log error if occurs here
        self.inner.get(col, key).ok()?
    }
    // with_get -> default is fine

    fn remove(&self, col: ColumnId, key: &[u8]) {
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

impl KeyValueDB for ReadOnlyDatabase {
    fn get(&self, col: u32, key: &[u8]) -> io::Result<Option<DBValue>> {
        self.inner.get(col, key)
    }

    fn get_by_prefix(&self, col: u32, prefix: &[u8]) -> Option<Box<[u8]>> {
        self.get_by_prefix(col, prefix)
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        panic!("Read only database")
    }

    fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
        let unboxed = self.inner.iter(col);
        Box::new(unboxed.into_iter())
    }

    fn iter_with_prefix<'a>(&'a self, col: u32, prefix: &'a [u8]) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
        self.inner.iter_with_prefix(col, prefix)
    }

    fn restore(&self, new_db: &str) -> io::Result<()> {
        self.inner.restore(new_db)
    }

    fn io_stats(&self, kind: kvdb::IoStatsKind) -> kvdb::IoStats {
        self.inner.io_stats(kind)
    }
}
