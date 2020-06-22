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

pub use frame_system::Trait as System;
use serde::{Deserialize, Serialize};
use sp_runtime::{
    generic::{Block as BlockT, SignedBlock},
    OpaqueExtrinsic,
};
use sp_storage::{StorageData, StorageKey};

/// Consolidation of substrate traits representing fundamental types
pub trait Substrate: System + Send + Sync + std::fmt::Debug {}

impl<T> Substrate for T where T: System + Send + Sync + std::fmt::Debug {}

/// A generic signed substrate block
pub type SubstrateBlock<T> = SignedBlock<NotSignedBlock<T>>;

/// Generic, unsigned block type
pub type NotSignedBlock<T> = BlockT<<T as System>::Header, OpaqueExtrinsic>;

// pub type Runtime<T, Run, Dis> = crate::backend::Runtime<T, Run, Dis>;

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

#[derive(Debug, Clone)]
pub struct Block<T: Substrate + Send + Sync> {
    pub inner: SubstrateBlock<T>,
    pub spec: u32,
}

// TODO: Possibly split block into extrinsics / digest / etc so that it can be sent in seperate parts to decode threads
impl<T> Block<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(block: SubstrateBlock<T>, spec: u32) -> Self {
        Self { inner: block, spec }
    }
}

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<T>
where
    T: Substrate + Send + Sync,
{
    inner: Vec<Block<T>>,
}

impl<T> BatchBlock<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(blocks: Vec<Block<T>>) -> Self {
        Self { inner: blocks }
    }

    pub fn inner(&self) -> &Vec<Block<T>> {
        &self.inner
    }
}

/// NewType for Storage Data
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Storage<T: Substrate + Send + Sync> {
    hash: T::Hash,
    block_num: u32,
    full_storage: bool,
    pub changes: Vec<(StorageKey, Option<StorageData>)>,
}

impl<T: Substrate + Send + Sync> Storage<T> {
    pub fn new(
        hash: T::Hash,
        block_num: u32,
        full_storage: bool,
        changes: Vec<(StorageKey, Option<StorageData>)>,
    ) -> Self {
        Self {
            block_num,
            hash,
            full_storage,
            changes,
        }
    }

    pub fn is_full(&self) -> bool {
        self.full_storage
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }

    pub fn hash(&self) -> &T::Hash {
        &self.hash
    }

    pub fn changes(&self) -> &[(StorageKey, Option<StorageData>)] {
        self.changes.as_slice()
    }
}
