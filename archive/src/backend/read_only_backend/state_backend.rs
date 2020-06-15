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

//! State Backend Interface

use super::ReadOnlyBackend;
use crate::error::Error as ArchiveError;

use super::HashOut;
use hash_db::{HashDB, Hasher, Prefix};
use kvdb::DBValue;
use sc_client_api::backend::StateBackend;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT, HashFor};
use sp_state_machine::TrieBackendStorage;
use sp_trie::{prefixed_key, PrefixedMemoryDB};

impl<Block: BlockT> StateBackend<HashFor<Block>> for ReadOnlyBackend<Block> {
    type Error = ArchiveError;
    type Transaction = ConsolidateTransaction;
    type TrieBackendStorage = Self;
}

/// Not supported
#[derive(Default, Clone)]
pub struct ConsolidateTransaction;

impl sp_state_machine::backend::Consolidate for ConsolidateTransaction {
    fn consolidate(&mut self, other: Self) {
        panic!("Consolidation of Transactions are not supported for Read Only Backend");
    }
}

impl<Block: BlockT> TrieBackendStorage<HashFor<Block>> for ReadOnlyBackend<Block> {
    type Overlay = StorageOverlay<HashFor<Block>>;

    fn get(&self, key: &HashOut<Block>, prefix: Prefix) -> Result<Option<DBValue>, String> {
        Ok(<Self as HashDB<HashFor<Block>>>::get(key, prefix))
    }
}

impl<H: Hasher> Default for StorageOverlay<H> {
    fn default() -> Self {
        Self {
            storage: PrefixedMemoryDB::default(),
            prefix_keys: true,
        }
    }
}

pub struct StorageOverlay<H: Hasher> {
    storage: PrefixedMemoryDB<H>,
    prefix_keys: bool,
}

impl<H: Hasher> hash_db::HashDB<H, DBValue> for StorageOverlay<H> {
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
        // TODO: Might be a problem,
        // needs more research into how TrieBackend/TrieBackendEssence/TrieBackendStorage
        // and the many different traits in Substrate work together
        if self.prefix_keys {
            let key = prefixed_key::<H>(key, prefix);
            self.storage.get(super::columns::STATE, &key)
        } else {
            self.storage.get(super::columns::STATE, key.as_ref())
        }
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        hash_db::HashDB::get(self, key, prefix).is_some()
    }

    fn insert(&mut self, _prefix: Prefix, _value: &[u8]) -> H::Out {
        panic!("Read Only Database; HashDB IMPL for StorageOverlay; insert(..)");
    }

    fn emplace(&mut self, _key: H::Out, _prefix: Prefix, _value: DBValue) {
        panic!("Read Only Database; HashDB IMPL for StorageOverlay; emplace(..)");
    }

    fn remove(&mut self, _key: &H::Out, _prefix: Prefix) {
        panic!("Read Only Database; HashDB IMPL for StorageOverlay; remove(..)");
    }
}

impl<H: Hasher> hash_db::HashDBRef<H, DBValue> for StorageOverlay<H> {
    fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
        hash_db::HashDB::get(self, key, prefix)
    }

    fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
        hash_db::HashDB::contains(self, key, prefix)
    }
}

impl<H: Hasher> hash_db::AsHashDB<H, DBValue> for StorageOverlay<H> {
    fn as_hash_db(&self) -> &(dyn hash_db::HashDB<H, DBValue>) {
        self
    }
    fn as_hash_db_mut(&mut self) -> &mut (dyn hash_db::HashDB<H, DBValue>) {
        panic!("Mutable references to database not allowed")
    }
}

impl<H: Hasher> sp_state_machine::backend::Consolidate for StorageOverlay<H> {
    fn consolidate(&mut self, other: Self) {
        panic!("Consolidation of Transactions are not supported for Read Only Backend");
    }
}
