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

//! Direct Database Type representations of types in `types.rs`
//! Only some types implemented, for convenience most types are already in their database model
//! equivalents

use std::marker::PhantomData;

use codec::{Decode, Encode, Error as DecodeError};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use sp_runtime::{
	generic::SignedBlock,
	traits::{Block as BlockT, Header as HeaderT},
};
use sp_storage::{StorageData, StorageKey};

use crate::types::*;

/// Struct modeling data returned from database when querying for a block
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub struct BlockModel {
	pub id: i32,
	pub parent_hash: Vec<u8>,
	pub hash: Vec<u8>,
	pub block_num: i32,
	pub state_root: Vec<u8>,
	pub extrinsics_root: Vec<u8>,
	pub digest: Vec<u8>,
	pub ext: Vec<u8>,
	pub spec: i32,
}

impl BlockModel {
	pub fn into_block_and_spec<B: BlockT>(self) -> Result<(B, u32), DecodeError> {
		let block_num = Decode::decode(&mut (self.block_num as u32).encode().as_slice())?;
		let extrinsics_root = Decode::decode(&mut self.extrinsics_root.as_slice())?;
		let state_root = Decode::decode(&mut self.state_root.as_slice())?;
		let parent_hash = Decode::decode(&mut self.parent_hash.as_slice())?;
		let digest = Decode::decode(&mut self.digest.as_slice())?;
		let ext = Decode::decode(&mut self.ext.as_slice())?;
		let header = <B::Header as HeaderT>::new(block_num, extrinsics_root, state_root, parent_hash, digest);

		let spec = self.spec as u32;

		Ok((B::new(header, ext), spec))
	}
}

/// Helper struct for decoding block modeling data into block type.
pub struct BlockModelDecoder<B: BlockT> {
	_marker: PhantomData<B>,
}

impl<'a, B: BlockT> BlockModelDecoder<B> {
	/// With a vector of BlockModel
	pub fn with_vec(blocks: Vec<BlockModel>) -> Result<Vec<Block<B>>, DecodeError> {
		blocks
			.into_iter()
			.map(|b| {
				let (block, spec) = b.into_block_and_spec()?;
				let block = SignedBlock { block, justifications: None };
				Ok(Block::new(block, spec))
			})
			.collect()
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StorageModel<Block: BlockT> {
	hash: Block::Hash,
	block_num: u32,
	full_storage: bool,
	key: StorageKey,
	data: Option<StorageData>,
}

impl<Block: BlockT> StorageModel<Block> {
	pub fn new(
		hash: Block::Hash,
		block_num: u32,
		full_storage: bool,
		key: StorageKey,
		data: Option<StorageData>,
	) -> Self {
		Self { hash, block_num, full_storage, key, data }
	}

	pub fn is_full(&self) -> bool {
		self.full_storage
	}

	pub fn block_num(&self) -> u32 {
		self.block_num
	}

	pub fn hash(&self) -> &Block::Hash {
		&self.hash
	}

	pub fn key(&self) -> &StorageKey {
		&self.key
	}

	pub fn data(&self) -> Option<&StorageData> {
		self.data.as_ref()
	}
}

impl<Block: BlockT> From<Storage<Block>> for Vec<StorageModel<Block>> {
	fn from(original: Storage<Block>) -> Vec<StorageModel<Block>> {
		let hash = *original.hash();
		let block_num = original.block_num();
		let full_storage = original.is_full();
		original
			.changes
			.into_iter()
			.map(|changes| StorageModel::new(hash, block_num, full_storage, changes.0, changes.1))
			.collect::<Vec<StorageModel<Block>>>()
	}
}

impl<Block: BlockT> From<BatchStorage<Block>> for Vec<StorageModel<Block>> {
	fn from(original: BatchStorage<Block>) -> Vec<StorageModel<Block>> {
		original.inner.into_iter().flat_map(Vec::<StorageModel<Block>>::from).collect()
	}
}
