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

use codec::{Decode, Encode};
use serde::{Deserialize, Serialize};
use xtra::Message;

use sp_runtime::{generic::SignedBlock, traits::Block as BlockT};
use sp_storage::{StorageData, StorageKey};

use crate::error::Result;

#[derive(Clone, Debug, Encode, Decode)]
pub struct Block<B: BlockT> {
	pub inner: SignedBlock<B>,
	pub spec: u32,
}

impl<B: BlockT> Block<B> {
	pub fn new(block: SignedBlock<B>, spec: u32) -> Self {
		Self { inner: block, spec }
	}
}

impl<B: BlockT> Message for Block<B> {
	type Result = ();
}

#[derive(Debug)]
pub struct Metadata {
	version: u32,
	meta: Vec<u8>,
}

impl Metadata {
	pub fn new(version: u32, meta: Vec<u8>) -> Self {
		Self { version, meta }
	}

	pub fn version(&self) -> u32 {
		self.version
	}

	pub fn meta(&self) -> &[u8] {
		self.meta.as_slice()
	}
}

impl Message for Metadata {
	type Result = ();
}

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<B: BlockT> {
	pub inner: Vec<Block<B>>,
}

impl<B: BlockT> BatchBlock<B> {
	pub fn new(blocks: Vec<Block<B>>) -> Self {
		Self { inner: blocks }
	}

	pub fn inner(&self) -> &Vec<Block<B>> {
		&self.inner
	}
}

impl<B: BlockT> Message for BatchBlock<B> {
	type Result = ();
}

/// NewType for Storage Data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Storage<Block: BlockT> {
	hash: Block::Hash,
	block_num: u32,
	full_storage: bool,
	pub changes: Vec<(StorageKey, Option<StorageData>)>,
}

impl<Block: BlockT> Storage<Block> {
	pub fn new(
		hash: Block::Hash,
		block_num: u32,
		full_storage: bool,
		changes: Vec<(StorageKey, Option<StorageData>)>,
	) -> Self {
		Self { block_num, hash, full_storage, changes }
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

	pub fn changes(&self) -> &[(StorageKey, Option<StorageData>)] {
		self.changes.as_slice()
	}
}

impl<Block: BlockT> Message for Storage<Block> {
	type Result = ();
}

#[derive(Debug)]
pub struct BatchStorage<B: BlockT> {
	pub inner: Vec<Storage<B>>,
}

impl<B: BlockT> BatchStorage<B> {
	pub fn new(storages: Vec<Storage<B>>) -> Self {
		Self { inner: storages }
	}

	pub fn inner(&self) -> &Vec<Storage<B>> {
		&self.inner
	}
}

impl<B: BlockT> Message for BatchStorage<B> {
	type Result = ();
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Die;
impl Message for Die {
	type Result = Result<()>;
}
