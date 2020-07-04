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

use crate::error::{ArchiveResult, Error as ArchiveError};
use codec::{Decode, Encode};
use serde::{Deserialize, Serialize};
use sp_runtime::{
    generic::{Block as NotSignedBlock, SignedBlock},
    traits::{Block as BlockT, NumberFor},
    OpaqueExtrinsic,
};
use sp_storage::{StorageData, StorageKey};
use xtra::prelude::*;

pub trait ThreadPool {
    type In: Ord + Eq + Clone + Send + Sync + Encode + Decode + std::hash::Hash + PriorityIdent;
    type Out: Send + Sync + std::fmt::Debug;
    fn add_task(&self, d: Vec<Self::In>, tx: flume::Sender<Self::Out>) -> ArchiveResult<usize>;
}

/// Get an identifier from data that can be used to sort it
pub trait PriorityIdent {
    type Ident: Eq + PartialEq + Send + Sync + Copy + Ord + PartialOrd;
    fn identifier(&self) -> Self::Ident;
}

#[async_trait::async_trait(?Send)]
pub trait Archive<B: BlockT> {
    /// start driving the execution of the archive
    async fn drive(&mut self) -> Result<(), ArchiveError>;

    /// this method will block indefinitely
    async fn block_until_stopped(&self) -> ();

    /// shutdown the system
    fn shutdown(self) -> Result<(), ArchiveError>;

    /// Get a reference to the context the actors are using
    fn context(&self) -> Result<super::actors::ActorContext<B>, ArchiveError>;
}

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
