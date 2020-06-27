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

use crate::error::Error as ArchiveError;
use serde::{Deserialize, Serialize};
use sp_runtime::{
    generic::{Block as NotSignedBlock, SignedBlock},
    traits::Block as BlockT,
    OpaqueExtrinsic,
};
use sp_storage::{StorageData, StorageKey};
use xtra::prelude::*;

// /// Generic, unsigned block type
// pub type AbstractBlock<B: BlockT> = NotSignedBlock<B::Header, OpaqueExtrinsic>;

// pub type Runtime<T, Run, Dis> = crate::backend::Runtime<T, Run, Dis>;

#[derive(Debug)]
pub struct Metadata {
    version: u32,
    meta: Vec<u8>,
}

impl Message for Metadata {
    type Result = Result<(), ArchiveError>;
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
pub struct Block<B: BlockT> {
    pub inner: SignedBlock<B>,
    pub spec: u32,
}

impl<B: BlockT> Message for Block<B> {
    type Result = Result<(), ArchiveError>;
}

// TODO: Possibly split block into extrinsics / digest / etc so that it can be sent in seperate parts to decode threads
impl<B: BlockT> Block<B> {
    pub fn new(block: SignedBlock<B>, spec: u32) -> Self {
        Self { inner: block, spec }
    }
}

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<B: BlockT> {
    inner: Vec<Block<B>>,
}

impl<B: BlockT> Message for BatchBlock<B> {
    type Result = Result<(), ArchiveError>;
}

impl<B: BlockT> BatchBlock<B> {
    pub fn new(blocks: Vec<Block<B>>) -> Self {
        Self { inner: blocks }
    }

    pub fn inner(&self) -> &Vec<Block<B>> {
        &self.inner
    }
}

/// NewType for Storage Data
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Storage<Block: BlockT> {
    hash: Block::Hash,
    block_num: u32,
    full_storage: bool,
    pub changes: Vec<(StorageKey, Option<StorageData>)>,
}

impl<Block: BlockT> Message for Storage<Block> {
    type Result = Result<(), ArchiveError>;
}

impl<Block: BlockT> Storage<Block> {
    pub fn new(
        hash: Block::Hash,
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

    pub fn hash(&self) -> &Block::Hash {
        &self.hash
    }

    pub fn changes(&self) -> &[(StorageKey, Option<StorageData>)] {
        self.changes.as_slice()
    }
}
