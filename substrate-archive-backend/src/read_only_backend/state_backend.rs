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

//! State Backend Interface

use std::marker::PhantomData;
use std::sync::Arc;

use hash_db::Prefix;
use kvdb::DBValue;

use sc_client_api::backend::StateBackend;
use sp_core::storage::ChildInfo;
use sp_runtime::traits::{Block as BlockT, HashFor};
use sp_state_machine::{StateMachineStats, TrieBackend, UsageInfo as StateUsageInfo};

use crate::database::ReadOnlyDb;

/// DB-backed patricia trie state, transaction type is an overlay of changes to commit.
pub type DbState<B> = TrieBackend<Arc<dyn sp_state_machine::Storage<HashFor<B>>>, HashFor<B>>;

/// Holds a reference to the disk backend
/// that trie operations can make use of
pub struct StateVault<Block: BlockT, D: ReadOnlyDb> {
	/// disk backend
	pub db: Arc<D>,
	prefix_keys: bool,
	_marker: PhantomData<Block>,
}

impl<Block, D> StateVault<Block, D>
where
	Block: BlockT,
	D: ReadOnlyDb,
{
	pub fn new(db: Arc<D>, prefix_keys: bool) -> Self {
		Self { db, prefix_keys, _marker: PhantomData }
	}
}

impl<Block, D> sp_state_machine::Storage<HashFor<Block>> for StateVault<Block, D>
where
	Block: BlockT,
	D: ReadOnlyDb,
{
	fn get(&self, key: &Block::Hash, prefix: Prefix) -> Result<Option<DBValue>, String> {
		if self.prefix_keys {
			let key = sp_trie::prefixed_key::<HashFor<Block>>(key, prefix);
			Ok(self.db.get(super::columns::STATE, &key))
		} else {
			Ok(self.db.get(super::columns::STATE, key.as_ref()))
		}
	}
}

/// TrieState
/// Returns a reference that implements StateBackend
/// It makes sure that the hash we are using stays pinned in storage
pub struct TrieState<Block: BlockT, D: ReadOnlyDb> {
	state: DbState<Block>,
	#[allow(unused)]
	storage: Arc<StateVault<Block, D>>,
	parent_hash: Option<Block::Hash>,
}

impl<B: BlockT, D: ReadOnlyDb> TrieState<B, D> {
	pub fn new(state: DbState<B>, storage: Arc<StateVault<B, D>>, parent_hash: Option<B::Hash>) -> Self {
		TrieState { state, storage, parent_hash }
	}
}

impl<B: BlockT, D: ReadOnlyDb> std::fmt::Debug for TrieState<B, D> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Block {:?}", self.parent_hash)
	}
}

impl<B: BlockT, D: ReadOnlyDb> StateBackend<HashFor<B>> for TrieState<B, D> {
	type Error = <DbState<B> as StateBackend<HashFor<B>>>::Error;
	type Transaction = <DbState<B> as StateBackend<HashFor<B>>>::Transaction;
	type TrieBackendStorage = <DbState<B> as StateBackend<HashFor<B>>>::TrieBackendStorage;

	fn storage(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
		self.state.storage(key)
	}

	fn storage_hash(&self, key: &[u8]) -> Result<Option<B::Hash>, Self::Error> {
		self.state.storage_hash(key)
	}

	fn child_storage(&self, child_info: &ChildInfo, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
		self.state.child_storage(child_info, key)
	}

	fn exists_storage(&self, key: &[u8]) -> Result<bool, Self::Error> {
		self.state.exists_storage(key)
	}

	fn exists_child_storage(&self, child_info: &ChildInfo, key: &[u8]) -> Result<bool, Self::Error> {
		self.state.exists_child_storage(child_info, key)
	}

	fn next_storage_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
		self.state.next_storage_key(key)
	}

	fn next_child_storage_key(&self, child_info: &ChildInfo, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
		self.state.next_child_storage_key(child_info, key)
	}

	fn apply_to_child_keys_while<F: FnMut(&[u8]) -> bool>(&self, child_info: &ChildInfo, f: F) {
		self.state.apply_to_child_keys_while(child_info, f)
	}

	fn for_keys_with_prefix<F: FnMut(&[u8])>(&self, prefix: &[u8], f: F) {
		self.state.for_keys_with_prefix(prefix, f)
	}

	fn for_key_values_with_prefix<F: FnMut(&[u8], &[u8])>(&self, prefix: &[u8], f: F) {
		self.state.for_key_values_with_prefix(prefix, f)
	}

	fn for_child_keys_with_prefix<F: FnMut(&[u8])>(&self, child_info: &ChildInfo, prefix: &[u8], f: F) {
		self.state.for_child_keys_with_prefix(child_info, prefix, f)
	}

	fn storage_root<'a>(
		&self,
		delta: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
	) -> (B::Hash, Self::Transaction)
	where
		B::Hash: Ord,
	{
		self.state.storage_root(delta)
	}

	fn child_storage_root<'a>(
		&self,
		child_info: &ChildInfo,
		delta: impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)>,
	) -> (B::Hash, bool, Self::Transaction)
	where
		B::Hash: Ord,
	{
		self.state.child_storage_root(child_info, delta)
	}

	fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
		self.state.pairs()
	}

	fn keys(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
		self.state.keys(prefix)
	}

	fn child_keys(&self, child_info: &ChildInfo, prefix: &[u8]) -> Vec<Vec<u8>> {
		self.state.child_keys(child_info, prefix)
	}

	fn as_trie_backend(&mut self) -> Option<&sp_state_machine::TrieBackend<Self::TrieBackendStorage, HashFor<B>>> {
		self.state.as_trie_backend()
	}

	fn register_overlay_stats(&self, stats: &StateMachineStats) {
		self.state.register_overlay_stats(stats);
	}

	fn usage_info(&self) -> StateUsageInfo {
		self.state.usage_info()
	}
}
