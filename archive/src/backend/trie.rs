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

use super::database::ReadOnlyDatabase;
use crate::types::{Substrate, NotSignedBlock};
use std::marker::PhantomData;
use sp_runtime::{generic::BlockId, traits::Header};
use sp_trie::{read_trie_value, Layout};
use frame_system::Trait as System;
use kvdb::DBValue;
use hash_db::{Hasher, Prefix};

pub struct StorageBackend<T: Substrate, H: Hasher> {
    db: ReadOnlyDatabase,
    _marker: PhantomData<(T, H)>
}

impl<T, H> StorageBackend<T, H> where T: Substrate + Send + Sync, H: Hasher {
    pub fn new(db: ReadOnlyDatabase) -> Self {
        Self { db, _marker: PhantomData }
    }

    // TODO: Gotta handle genesis storage
    pub fn storage(&self, state_hash: T::Hash, key: &[u8]) -> Option<Vec<u8>> {
        // db / root / key
        // flesh it out
        let header = super::util::read_header::<NotSignedBlock<T>>(
            &self.db, 
            columns::KEY_LOOKUP, 
            columns::HEADER, 
            BlockId::Hash(state_hash)
        ).expect("Header Metadata Lookup Failed").expect("Header not found!");
        let root = header.state_root();

        let val = read_trie_value::<Layout<<T as System>::Hashing>, _>(self, root, key)
            .expect("Read Trie Value Error");
        
        // sp_trie::read_trie_value()
        Some(Vec::new())
    }
}

impl<T: Substrate + Send + Sync, H: Hasher> hash_db::HashDB<H, DBValue> for StorageBackend<T, H> {
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
        // TODO: Might be as proble, don't know how hashdb interacts with KVDB
        match self.db.get(*key, prefix) {
            Ok(x) => x,
            Err(e) => {
                log::warn!("Failed to read from DB: {}", e);
                None
            },
        }
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        hash_db::HashDB::get(self, key, prefix).is_some()
    }

    fn insert(&mut self, prefix: Prefix, value: &[u8]) -> bool {
        panic!("Read Only Database; HashDB IMPL for StorageBackend; insert(..)");
    }

    fn emplace(&mut self, key: H::Out, prefix: Prefix, value: DBValue) {
        panic!("Read Only Database; HashDB IMPL for StorageBackend; emplace(..)");
    }

    fn remove(&mut self, key: &H::Out, prefix: Prefix) {
        panic!("Read Only Database; HashDB IMPL for StorageBackend; remove(..)");
    }
}

impl<T: Substrate + Send + Sync, H: Hasher> hash_db::HashDBRef<H, DBValue> for StorageBackend<T, H> {
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
        hash_db::HashDB::get(self, key, prefix)
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        hash_db::HashDB::contains(self, key, prefix)
    }
}

impl<T: Substrate + Send + Sync, H: Hasher> hash_db::AsHashDB<H, DBValue> for StorageBackend<T, H> {
    fn as_hash_db(&self) -> &(dyn hash_db::HashDB<H, DBValue>) { self }
    fn as_hash_db_mut(&mut self) -> &mut (dyn hash_db::HashDB<H, DBValue>) { panic!("Mutable references to database not allowed") }
}

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
