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

//! Backend that wraps Parity Trie Implementation to retrieve
//! values from RocksDB.
//! Preferred over using the Substrate Client, since this is a lighter-weight implementation
//! that gives substrate-archive more fine-grained control over implementation and optimization details.
//! Especially with regards to reading.
//! According to benchmarks, performance improvements are currently neglible. Mostly because the code has only
//! been slightly modified so far.
//! This only wraps functions that are needed for substrate archive, avoiding needing to import entire runtimes.

mod blockchain_backend;
mod main_backend;
mod misc_backend;
mod state_backend;

pub use self::state_backend::TrieState;
use self::state_backend::{DbState, StateVault};
use super::database::ReadOnlyDatabase;
use super::util::columns;
use crate::error::Result;
use codec::Decode;
use hash_db::Prefix;
use kvdb::DBValue;
use sc_client_api::backend::StateBackend;
use sp_blockchain::{Backend as _, HeaderBackend as _};
use sp_runtime::{
    generic::{BlockId, SignedBlock},
    traits::{Block as BlockT, HashFor, Header},
    Justification,
};
use std::{convert::TryInto, sync::Arc};

pub struct ReadOnlyBackend<Block: BlockT> {
    db: Arc<ReadOnlyDatabase>,
    storage: Arc<StateVault<Block>>,
}

impl<Block> ReadOnlyBackend<Block>
where
    Block: BlockT,
{
    pub fn new(db: Arc<ReadOnlyDatabase>, prefix_keys: bool) -> Self {
        let vault = Arc::new(StateVault::new(db.clone(), prefix_keys));
        Self { db, storage: vault }
    }

    /// get a reference to the backing database
    pub fn backing_db(&self) -> Arc<ReadOnlyDatabase> {
        self.db.clone()
    }

    fn state_at(&self, hash: Block::Hash) -> Option<TrieState<Block>> {
        // genesis
        if hash == Default::default() {
            let genesis_storage = DbGenesisStorage::<Block>(Block::Hash::default());
            let root = Block::Hash::default();
            let state = DbState::<Block>::new(Arc::new(genesis_storage), root);
            Some(TrieState::<Block>::new(
                state,
                self.storage.clone(),
                Some(Block::Hash::default()),
            ))
        } else if let Some(state_root) = self.state_root(hash) {
            let state = DbState::<Block>::new(self.storage.clone(), state_root);
            Some(TrieState::<Block>::new(
                state,
                self.storage.clone(),
                Some(hash),
            ))
        } else {
            None
        }
    }

    /// get the state root for a block
    fn state_root(&self, hash: Block::Hash) -> Option<Block::Hash> {
        // db / root / key
        // flesh it out
        let header = super::util::read_header::<Block>(
            &self.db,
            columns::KEY_LOOKUP,
            columns::HEADER,
            BlockId::Hash(hash),
        )
        .expect("Header metadata lookup failed");
        header.map(|h| *h.state_root())
    }

    /// gets storage for some block hash
    pub fn storage(&self, hash: Block::Hash, key: &[u8]) -> Option<Vec<u8>> {
        match self.state_at(hash) {
            Some(state) => state
                .storage(key)
                .unwrap_or_else(|_| panic!("No storage found for {:?}", hash)),
            None => None,
        }
    }

    pub fn storage_hash(&self, hash: Block::Hash, key: &[u8]) -> Option<Block::Hash> {
        match self.state_at(hash) {
            Some(state) => state
                .storage_hash(key)
                .unwrap_or_else(|_| panic!("No storage found for {:?}", hash)),
            None => None,
        }
    }

    /// get storage keys for a prefix at a block in time
    pub fn storage_keys(&self, hash: Block::Hash, prefix: &[u8]) -> Option<Vec<Vec<u8>>> {
        match self.state_at(hash) {
            Some(state) => Some(state.keys(prefix)),
            None => None,
        }
    }

    /// Get a block from the canon chain
    /// This also tries to catch up with the primary rocksdb instance
    pub fn block(&self, id: &BlockId<Block>) -> Option<SignedBlock<Block>> {
        let header = self.header(*id).ok()?;
        let body = self.body(*id).ok()?;
        let justification = self.justification(*id).ok()?;
        construct_block(header, body, justification)
    }

    /// Iterate over all blocks that match the predicate `fun`
    /// Tries to iterates over the latest version of the database.
    /// The predicate exists to reduce database reads
    pub fn iter_blocks<'a>(
        &'a self,
        fun: impl Fn(u32) -> bool + 'a,
    ) -> Result<impl Iterator<Item = SignedBlock<Block>> + 'a> {
        let readable_db = self.db.clone();
        self.db.try_catch_up_with_primary()?;
        Ok(self
            .db
            .iter(super::util::columns::KEY_LOOKUP)
            .take_while(|(_, value)| !value.is_empty())
            .filter_map(move |(key, value)| {
                let arr: &[u8; 4] = key[0..4].try_into().ok()?;
                let num = u32::from_be_bytes(*arr);
                if key.len() == 4 && fun(num) {
                    let head: Option<Block::Header> = readable_db
                        .get(super::util::columns::HEADER, &value)
                        .map(|bytes| Decode::decode(&mut &bytes[..]).ok())
                        .flatten();
                    let body: Option<Vec<Block::Extrinsic>> = readable_db
                        .get(super::util::columns::BODY, &value)
                        .map(|bytes| Decode::decode(&mut &bytes[..]).ok())
                        .flatten();
                    let justif: Option<Justification> = readable_db
                        .get(super::util::columns::JUSTIFICATION, &value)
                        .map(|bytes| Decode::decode(&mut &bytes[..]).ok())
                        .flatten();
                    construct_block(head, body, justif)
                } else {
                    None
                }
            }))
    }
}

struct DbGenesisStorage<Block: BlockT>(pub Block::Hash);
impl<Block: BlockT> sp_state_machine::Storage<HashFor<Block>> for DbGenesisStorage<Block> {
    fn get(
        &self,
        _key: &Block::Hash,
        _prefix: Prefix,
    ) -> std::result::Result<Option<DBValue>, String> {
        Ok(None)
    }
}

fn construct_block<Block: BlockT>(
    header: Option<Block::Header>,
    body: Option<Vec<Block::Extrinsic>>,
    justification: Option<Justification>,
) -> Option<SignedBlock<Block>> {
    match (header, body, justification) {
        (Some(header), Some(extrinsics), justification) => Some(SignedBlock {
            block: Block::new(header, extrinsics),
            justification,
        }),
        _ => None,
    }
}

#[cfg(feature = "test_rocksdb")]
mod tests {
    #[allow(unused)]
    use super::*;
    use crate::backend::test_util::harness;
    use crate::{twox_128, StorageKey};
    use codec::Decode;
    use polkadot_service::Block;
    use primitive_types::H256;
    use std::time::Instant;
    // change this to run tests
    const DB: &'static str = "/home/insipx/.local/share/polkadot/chains/ksmcc3/db";

    fn balances_freebalance_key() -> StorageKey {
        let balances_key = twox_128(b"Balances").to_vec();
        let freebalance_key = twox_128(b"FreeBalance").to_vec();
        let mut balances_freebalance = balances_key.clone();
        balances_freebalance.extend(freebalance_key);
        StorageKey(balances_freebalance)
    }

    #[test]
    fn should_create_new() {
        harness(DB, |db| {
            ReadOnlyBackend::<Block>::new(db, false);
        });
    }

    /// Gets the FreeBalance of an account from block 10,000 on Kusama CC3
    /// Must have a RocksDB database with a node that has been synced up to block 10,000
    /// 0xae327b35880aa9d028370f063e4c4d666f6bad89800dd979ca8b9dbf064393d0 is the hash of the block in use
    #[test]
    fn should_get() {
        let key1 = "c2261276cc9d1f8598ea4b6a74b15c2f6482b9ade7bc6657aaca787ba1add3b4004def926cde2699f236c59fa11909a2aee554f0fe56fb88b9a9604669a200a9";
        let key1 = hex::decode(key1).unwrap();

        let key2 = "c2261276cc9d1f8598ea4b6a74b15c2f6482b9ade7bc6657aaca787ba1add3b4fe8ec31f36f7c3c4a4372f738bb7809d3aa5f533f46b3637458a630746b304a8";
        let key2 = hex::decode(key2).unwrap();

        let hash = hex::decode("ae327b35880aa9d028370f063e4c4d666f6bad89800dd979ca8b9dbf064393d0")
            .unwrap();
        let hash = H256::from_slice(hash.as_slice());

        harness(DB, |db| {
            let db = ReadOnlyBackend::<Block>::new(db, true);
            let time = Instant::now(); // FIXME: bootleg benchmark.
            let val = db.storage(hash, key1.as_slice()).unwrap();
            let elapsed = time.elapsed();
            println!(
                "Took {} seconds, {} milli-seconds, {} nano-seconds",
                elapsed.as_secs(),
                elapsed.as_millis(),
                elapsed.as_nanos()
            );
            let val: u128 = Decode::decode(&mut val.as_slice()).unwrap();
            assert_eq!(10379170000000000, val);
            let val = db.storage(hash, key2.as_slice()).unwrap();
            let val: u128 = Decode::decode(&mut val.as_slice()).unwrap();
            assert_eq!(226880000000000, val);
        });
    }

    #[test]
    fn should_get_keys() {
        let hash = hex::decode("ae327b35880aa9d028370f063e4c4d666f6bad89800dd979ca8b9dbf064393d0")
            .unwrap();
        let hash = H256::from_slice(hash.as_slice());
        let key = balances_freebalance_key();

        harness(DB, |db| {
            let db = ReadOnlyBackend::<Block>::new(db, true);
            let time = Instant::now();
            let keys = db.storage_keys(hash, key.0.as_slice());
            let elapsed = time.elapsed();
            println!(
                "Took {} seconds, {} milli-seconds, {} nano-seconds",
                elapsed.as_secs(),
                elapsed.as_millis(),
                elapsed.as_nanos()
            );
        });
    }
}
