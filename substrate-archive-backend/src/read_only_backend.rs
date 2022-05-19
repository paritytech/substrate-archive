// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use std::{convert::TryInto, sync::Arc};

use codec::Decode;
use hash_db::Prefix;
use kvdb::DBValue;

use sc_client_api::backend::StateBackend;
use sc_service::TransactionStorageMode;
use sp_blockchain::{Backend as _, HeaderBackend as _};
use sp_runtime::{
	generic::{BlockId, SignedBlock},
	traits::{Block as BlockT, HashFor, Header as HeaderT},
	Justifications,
};

pub use self::state_backend::TrieState;
use self::state_backend::{DbState, StateVault};
use crate::{database::ReadOnlyDb, error::Result, util::columns};

pub struct ReadOnlyBackend<Block, D> {
	db: Arc<D>,
	storage: Arc<StateVault<Block, D>>,
	storage_mode: TransactionStorageMode,
}

impl<Block, D> ReadOnlyBackend<Block, D>
where
	Block: BlockT,
	Block::Header: HeaderT,
	D: ReadOnlyDb + 'static,
{
	pub fn new(db: Arc<D>, prefix_keys: bool, storage_mode: TransactionStorageMode) -> Self {
		let vault = Arc::new(StateVault::new(db.clone(), prefix_keys));
		Self { db, storage: vault, storage_mode }
	}

	/// get a reference to the backing database
	pub fn backing_db(&self) -> Arc<D> {
		self.db.clone()
	}

	fn state_at(&self, hash: Block::Hash) -> Option<TrieState<Block, D>> {
		// genesis
		if hash == Default::default() {
			let genesis_storage = DbGenesisStorage::<Block>(Block::Hash::default());
			let root = Block::Hash::default();
			let state = DbState::<Block>::new(Arc::new(genesis_storage), root);
			Some(TrieState::<Block, D>::new(state, self.storage.clone(), Some(Block::Hash::default())))
		} else if let Some(state_root) = self.state_root(hash) {
			let state = DbState::<Block>::new(self.storage.clone(), state_root);
			Some(TrieState::<Block, D>::new(state, self.storage.clone(), Some(hash)))
		} else {
			None
		}
	}

	/// get the state root for a block
	fn state_root(&self, hash: Block::Hash) -> Option<Block::Hash> {
		// db / root / key
		// flesh it out
		let header =
			super::util::read_header::<Block, D>(&*self.db, columns::KEY_LOOKUP, columns::HEADER, BlockId::Hash(hash))
				.expect("Header metadata lookup failed");
		header.map(|h| *h.state_root())
	}

	/// gets storage for some block hash
	pub fn storage(&self, hash: Block::Hash, key: &[u8]) -> Option<Vec<u8>> {
		match self.state_at(hash) {
			Some(state) => state.storage(key).unwrap_or_else(|_| panic!("No storage found for {:?}", hash)),
			None => None,
		}
	}

	/// Get keyed storage value hash or None if there is nothing associated.
	pub fn storage_hash(&self, hash: Block::Hash, key: &[u8]) -> Option<Block::Hash> {
		match self.state_at(hash) {
			Some(state) => state.storage_hash(key).unwrap_or_else(|_| panic!("No storage found for {:?}", hash)),
			None => None,
		}
	}

	/// get storage keys for a prefix at a block in time
	pub fn storage_keys(&self, hash: Block::Hash, prefix: &[u8]) -> Option<Vec<Vec<u8>>> {
		self.state_at(hash).map(|state| state.keys(prefix))
	}

	/// Get a block from the canon chain
	/// This also tries to catch up with the primary rocksdb instance
	pub fn block(&self, id: &BlockId<Block>) -> Option<SignedBlock<Block>> {
		let header = self.header(*id).ok()?;
		let body = self.body(*id).ok()?;
		let justifications = self.justifications(*id).ok()?;
		construct_block(header, body, justifications)
	}

	/// Iterate over all blocks that match the predicate `fun`
	/// Tries to iterates over the latest version of the database.
	/// The predicate exists to reduce database reads
	pub fn iter_blocks<'a>(
		&'a self,
		fun: impl Fn(u32) -> bool + 'a,
	) -> Result<impl Iterator<Item = SignedBlock<Block>> + 'a> {
		let readable_db = self.db.clone();
		self.db.catch_up_with_primary()?;
		Ok(self.db.iter(super::util::columns::KEY_LOOKUP).take_while(|(_, value)| !value.is_empty()).filter_map(
			move |(key, value)| {
				let arr: &[u8; 4] = key[0..4].try_into().ok()?;
				let num = u32::from_be_bytes(*arr);
				if key.len() == 4 && fun(num) {
					let head: Option<Block::Header> = readable_db
						.get(super::util::columns::HEADER, &value)
						.and_then(|bytes| Decode::decode(&mut &bytes[..]).ok());
					let body: Option<Vec<Block::Extrinsic>> = readable_db
						.get(super::util::columns::BODY, &value)
						.and_then(|bytes| Decode::decode(&mut &bytes[..]).ok());
					let justif: Option<Justifications> = readable_db
						.get(super::util::columns::JUSTIFICATION, &value)
						.and_then(|bytes| Decode::decode(&mut &bytes[..]).ok());
					construct_block(head, body, justif)
				} else {
					None
				}
			},
		))
	}
}

struct DbGenesisStorage<Block: BlockT>(pub Block::Hash);
impl<Block: BlockT> sp_state_machine::Storage<HashFor<Block>> for DbGenesisStorage<Block> {
	fn get(&self, _key: &Block::Hash, _prefix: Prefix) -> std::result::Result<Option<DBValue>, String> {
		Ok(None)
	}
}

fn construct_block<Block: BlockT>(
	header: Option<Block::Header>,
	body: Option<Vec<Block::Extrinsic>>,
	justifications: Option<Justifications>,
) -> Option<SignedBlock<Block>> {
	match (header, body, justifications) {
		(Some(header), Some(extrinsics), justifications) => {
			Some(SignedBlock { block: Block::new(header, extrinsics), justifications })
		}
		_ => None,
	}
}
