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
use crate::types::Substrate;
use std::marker::PhantomData;
use sp_runtime::{generic::BlockId, traits::{Header, HashFor, Block as BlockT}};
use sp_trie::{read_trie_value, prefixed_key, Layout};
use kvdb::DBValue;
use hash_db::{Hasher, Prefix};

pub struct StorageBackend<Block: BlockT> {
    db: ReadOnlyDatabase,
    prefix_keys: bool,
    _marker: PhantomData<Block>
}

impl<Block> StorageBackend<Block> where Block: BlockT {
    pub fn new(db: ReadOnlyDatabase, prefix_keys: bool) -> Self {
        Self { db, prefix_keys, _marker: PhantomData }
    }

    // TODO: Gotta handle genesis storage
    pub fn storage(&self, state_hash: Block::Hash, key: &[u8]) -> Option<Vec<u8>> {
        // db / root / key
        // flesh it out
        let header = super::util::read_header::<Block>(
            &self.db, 
            columns::KEY_LOOKUP, 
            columns::HEADER, 
            BlockId::Hash(state_hash)
        ).expect("Header Metadata Lookup Failed").expect("Header not found!");
        let root = header.state_root();

        let val = read_trie_value::<Layout<HashFor<Block>>, _>(self, root, key)
            .expect("Read Trie Value Error");
        
        val
    }
}

type HashOut<Block> = <HashFor<Block> as Hasher>::Out;

impl<Block: BlockT> hash_db::HashDB<HashFor<Block>, DBValue> for StorageBackend<Block> {
    fn get(&self, key: &HashOut<Block>, prefix: Prefix) -> Option<DBValue> {
        // TODO: Might be a problem, don't know how hashdb interacts with KVDB
        if self.prefix_keys {
            let key = prefixed_key::<HashFor<Block>>(key, prefix);
            self.db.get(columns::STATE, &key)
        }  else {
            self.db.get(columns::STATE, key.as_ref())
        }
    }

    fn contains(&self, key: &HashOut<Block>, prefix: Prefix) -> bool {
        hash_db::HashDB::get(self, key, prefix).is_some()
    }

    fn insert(&mut self, _prefix: Prefix, _value: &[u8]) -> HashOut<Block> {
        panic!("Read Only Database; HashDB IMPL for StorageBackend; insert(..)");
    }

    fn emplace(&mut self, _key: HashOut<Block>, _prefix: Prefix, _value: DBValue) {
        panic!("Read Only Database; HashDB IMPL for StorageBackend; emplace(..)");
    }

    fn remove(&mut self, _key: &HashOut<Block>, _prefix: Prefix) {
        panic!("Read Only Database; HashDB IMPL for StorageBackend; remove(..)");
    }
}

impl<Block: BlockT> hash_db::HashDBRef<HashFor<Block>, DBValue> for StorageBackend<Block> {
    fn get(&self, key: &HashOut<Block>, prefix: Prefix) -> Option<DBValue> {
        hash_db::HashDB::get(self, key, prefix)
    }

    fn contains(&self, key: &HashOut<Block>, prefix: Prefix) -> bool {
        hash_db::HashDB::contains(self, key, prefix)
    }
}

impl<Block: BlockT> hash_db::AsHashDB<HashFor<Block>, DBValue> for StorageBackend<Block> {
    fn as_hash_db(&self) -> &(dyn hash_db::HashDB<HashFor<Block>, DBValue>) { self }
    fn as_hash_db_mut(&mut self) -> &mut (dyn hash_db::HashDB<HashFor<Block>, DBValue>) { panic!("Mutable references to database not allowed") }
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

#[cfg(test)]
mod tests {
    use super::*;

}