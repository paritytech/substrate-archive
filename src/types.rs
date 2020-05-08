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

mod traits;
use codec::Encode;
use desub::decoder::Metadata;
use sp_core::storage::{StorageChangeSet, StorageData};
use sp_runtime::{
    generic::{Block as BlockT, SignedBlock},
    traits::{Block as _, Header as _},
};
use subxt::system::System;

pub use self::traits::ChainInfo;
pub use self::traits::Substrate;

/// A generic substrate block
pub type SubstrateBlock<T> = SignedBlock<BlockT<<T as System>::Header, <T as System>::Extrinsic>>;

/// Just one of those low-life not-signed types
pub type NotSignedBlock<T> = BlockT<<T as System>::Header, <T as System>::Extrinsic>;

/// Read-Only RocksDb backed Backend Type
pub type ArchiveBackend<T> = sc_client_db::Backend<NotSignedBlock<T>>;

#[derive(Debug)]
pub enum BatchData<T: Substrate> {
    BatchBlock(BatchBlock<T>),
    BatchStorage(BatchStorage<T>),
}

impl<T> BatchData<T>
where
    T: Substrate,
{
    pub fn hashes(&self) -> Vec<T::Hash> {
        match self {
            BatchData::BatchBlock(b) => b
                .inner()
                .iter()
                .map(|b| b.inner.block.header.hash())
                .collect::<Vec<T::Hash>>(),
            BatchData::BatchStorage(s) => s
                .inner()
                .iter()
                .map(|s| *s.hash())
                .collect::<Vec<T::Hash>>(),
        }
    }
}

impl<T> ChainInfo<T> for Data<T>
where
    T: Substrate,
{
    fn get_hash(&self) -> T::Hash {
        match self {
            Data::Header(h) | Data::FinalizedHead(h) => *h.hash(),
            Data::Block(b) => b.inner.block.header.hash(),
            Data::Storage(s) => *s.hash(),
            Data::Event(e) => e.hash(),
        }
    }
}

/// Sent from Substrate API to be committed into the Database
#[derive(Debug)]
pub enum Data<T: Substrate> {
    Header(Header<T>),
    FinalizedHead(Header<T>),
    Block(Block<T>),
    Storage(Storage<T>),
    Event(Event<T>),
}

// new types to allow implementing of traits
// NewType for Header
#[derive(Debug)]
pub struct Header<T: Substrate> {
    inner: T::Header,
}

impl<T: Substrate> Header<T> {
    pub fn new(header: T::Header) -> Self {
        Self { inner: header }
    }

    pub fn inner(&self) -> &T::Header {
        &self.inner
    }

    pub fn hash(&self) -> &T::Hash {
        self.hash()
    }
}

#[derive(Debug, Clone)]
pub struct Block<T: Substrate> {
    pub inner: SubstrateBlock<T>,
    pub meta: Metadata,
    pub spec: u32,
}

impl<T> ChainInfo<T> for Block<T>
where
    T: Substrate,
{
    fn get_hash(&self) -> T::Hash {
        self.inner.block.header().hash()
    }
}
// TODO: Possibly split block into extrinsics / digest / etc so that it can be sent in seperate parts to decode threads
impl<T> Block<T>
where
    T: Substrate,
{
    pub fn new(block: SubstrateBlock<T>, meta: Metadata, spec: u32) -> Self {
        Self {
            inner: block,
            meta,
            spec,
        }
    }

    pub fn inner(&self) -> &SubstrateBlock<T> {
        &self.inner
    }
}

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<T: Substrate> {
    inner: Vec<Block<T>>,
}

impl<T: Substrate> BatchBlock<T> {
    pub fn new(blocks: Vec<Block<T>>) -> Self {
        Self { inner: blocks }
    }

    pub fn inner(&self) -> &Vec<Block<T>> {
        &self.inner
    }
}

impl<T: Substrate> From<BatchBlock<T>> for Vec<Vec<Extrinsic<T>>> {
    fn from(batch_block: BatchBlock<T>) -> Vec<Vec<Extrinsic<T>>> {
        batch_block.inner().iter().map(|b| b.into()).collect()
    }
}

#[derive(Debug)]
pub struct Extrinsic<T: Substrate + Send + Sync> {
    pub inner: Vec<u8>,
    pub hash: T::Hash,
    pub spec: u32,
    pub meta: Metadata,
}

impl<T: Substrate + Send + Sync> From<&Block<T>> for Vec<Extrinsic<T>> {
    fn from(block: &Block<T>) -> Vec<Extrinsic<T>> {
        let block = block.clone();
        let hash = block.get_hash();
        let spec = block.spec;
        let meta = block.meta.clone();
        block
            .inner()
            .block
            .extrinsics
            .iter()
            .map(move |e| Extrinsic {
                inner: e.encode(),
                hash,
                spec,
                meta: meta.clone(),
            })
            .collect()
    }
}

/// newType for Storage Data
#[derive(Debug)]
pub struct Storage<T: Substrate> {
    data: StorageData,
    hash: T::Hash,
    // meta: StorageMetadata,
}

impl<T> Storage<T>
where
    T: Substrate,
{
    pub fn new(data: StorageData, hash: T::Hash /* meta: StorageMetadata */) -> Self {
        Self {
            data,
            hash, /*, meta */
        }
    }

    pub fn data(&self) -> &StorageData {
        &self.data
    }

    pub fn hash(&self) -> &T::Hash {
        &self.hash
    }
}

/// NewType for committing many storage items into the database at once
#[derive(Debug)]
pub struct BatchStorage<T: Substrate> {
    inner: Vec<Storage<T>>,
}

impl<T> BatchStorage<T>
where
    T: Substrate,
{
    pub fn new(data: Vec<Storage<T>>) -> Self {
        Self { inner: data }
    }

    pub fn inner(&self) -> &Vec<Storage<T>> {
        &self.inner
    }

    pub fn consume(self) -> Vec<Storage<T>> {
        self.inner
    }
}

/// NewType for committing Events to the database
#[derive(Debug, PartialEq, Eq)]
pub struct Event<T: Substrate> {
    change_set: StorageChangeSet<T::Hash>,
}

impl<T: Substrate> Event<T> {
    pub fn new(change_set: StorageChangeSet<T::Hash>) -> Self {
        Self { change_set }
    }

    pub fn change_set(&self) -> &StorageChangeSet<T::Hash> {
        &self.change_set
    }

    pub fn hash(&self) -> T::Hash {
        self.change_set.block
    }
}
