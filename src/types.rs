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

use crate::error::Result;
use codec::{Decode, Encode};
use serde::{Deserialize, Serialize};
use sp_runtime::{generic::SignedBlock, traits::Block as BlockT};
use sp_storage::{StorageData, StorageKey};

pub trait ThreadPool: Send + Sync {
    type In: Send + Sync + std::fmt::Debug;
    type Out: Send + Sync + std::fmt::Debug;
    /// Adds a task to the threadpool.
    /// Should not block!
    fn add_task(&self, d: Vec<Self::In>, tx: flume::Sender<Self::Out>) -> Result<usize>;
}

#[async_trait::async_trait(?Send)]
pub trait Archive<B: BlockT + Unpin>
where
    B::Hash: Unpin,
{
    /// start driving the execution of the archive
    fn drive(&mut self) -> Result<()>;

    /// this method will block indefinitely
    async fn block_until_stopped(&self) -> ();

    /// shutdown the system
    fn shutdown(self) -> Result<()>;

    /// Shutdown the system when self is boxed (useful when erasing the types of the runtime)
    fn boxed_shutdown(self: Box<Self>) -> Result<()>;

    /// Get a reference to the context the actors are using
    fn context(&self) -> Result<super::actors::ActorContext<B>>;
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

#[derive(Encode, Decode, Debug, Clone)]
pub struct Block<B: BlockT> {
    pub inner: SignedBlock<B>,
    pub spec: u32,
}

impl<B: BlockT> Block<B> {
    pub fn new(block: SignedBlock<B>, spec: u32) -> Self {
        Self { inner: block, spec }
    }
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

/// NewType for Storage Data
#[derive(Clone, Serialize, Deserialize, Debug)]
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

/// Enum of standard substrate Pallets/Frames 
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Frame {
    System,
}

impl std::fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::System => write!(f, "{}", "frame_system"),
        }
    }
}

/// Holds storage entries that originated in a specific substrate pallet
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FrameEntry<K, V> {
    /// the Postgres table to insert this value into. Corresponds with the name 
    /// of the pallet that this storage value originated in
    table: Frame,
    /// Block number the storage entry is from
    block_num: u32,
    /// Hash of the block the storage entry is from
    hash: Vec<u8>,
    // TODO: should support map/double-map types explicitly via an enum?
    /// Key of the storage entry
    key: K,
    /// Value of the storage entry
    value: Option<V>,
}

impl<K, V> FrameEntry<K, V> {
    pub fn new(table: Frame, block_num: u32, hash: Vec<u8>, key: K, value: Option<V>) -> Self {
        Self {
            table,
            block_num,
            hash,
            key,
            value 
        }
    }

    /// Get a reference to the key of this storage entry 
    pub fn key(&self) -> &K {
        &self.key
    }
    
    /// Get a reference to the value of this storage entry
    pub fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }
    
    /// Get a reference to the hash, as a slice of bytes, of this storage entry
    pub fn hash(&self) -> &[u8] {
        self.hash.as_slice()
    }
    
    /// get the block number this storage entry originated in
    pub fn block_num(&self) -> u32 {
        self.block_num
    }
    
    /// Get the table to insert this storage entry into
    pub fn table(&self) -> &Frame {
        &self.table
    }
}

