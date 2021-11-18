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

use crate::database::models::ExtrinsicsModel;

pub trait Hash: Copy + Send + Sync + Unpin + AsRef<[u8]> + 'static {}

impl<T> Hash for T where T: Copy + Send + Sync + Unpin + AsRef<[u8]> + 'static {}

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

#[derive(Clone, Debug, Encode, Decode)]
pub struct Block<B> {
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

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<B> {
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
pub struct Storage<Hash> {
	hash: Hash,
	block_num: u32,
	full_storage: bool,
	pub changes: Vec<(StorageKey, Option<StorageData>)>,
}

impl<Hash> Storage<Hash> {
	pub fn new(
		hash: Hash,
		block_num: u32,
		full_storage: bool,
		changes: Vec<(StorageKey, Option<StorageData>)>,
	) -> Self {
		Self { hash, block_num, full_storage, changes }
	}

	pub fn is_full(&self) -> bool {
		self.full_storage
	}

	pub fn block_num(&self) -> u32 {
		self.block_num
	}

	pub fn hash(&self) -> &Hash {
		&self.hash
	}

	pub fn changes(&self) -> &[(StorageKey, Option<StorageData>)] {
		self.changes.as_slice()
	}
}

impl<Hash: Send + 'static> Message for Storage<Hash> {
	type Result = ();
}

#[derive(Debug)]
pub struct BatchStorage<Hash> {
	pub inner: Vec<Storage<Hash>>,
}

impl<Hash> BatchStorage<Hash> {
	pub fn new(storages: Vec<Storage<Hash>>) -> Self {
		Self { inner: storages }
	}

	pub fn inner(&self) -> &Vec<Storage<Hash>> {
		&self.inner
	}
}

impl<Hash: Send + Sync + 'static> Message for BatchStorage<Hash> {
	type Result = ();
}

#[derive(Debug)]
pub struct BatchExtrinsics {
	pub inner: Vec<ExtrinsicsModel>,
}

impl BatchExtrinsics {
	pub fn new(extrinsics: Vec<ExtrinsicsModel>) -> Self {
		Self { inner: extrinsics }
	}

	pub fn inner(self) -> Vec<ExtrinsicsModel> {
		self.inner
	}

	pub fn len(&self) -> usize {
		self.inner.len()
	}
}

impl Message for BatchExtrinsics {
	type Result = ();
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Die;
impl Message for Die {
	type Result = ();
}
