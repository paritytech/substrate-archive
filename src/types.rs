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
use desub::decoder::{GenericExtrinsic, GenericSignature, GenericCall, ExtrinsicArgument, Metadata};
use sp_core::storage::{StorageChangeSet, StorageData};
use sp_runtime::{
    generic::{Block as BlockT, SignedBlock},
    traits::{Block as _, Header as _},
};
use subxt::system::System;

pub use self::traits::Substrate;

/// A generic substrate block
pub type SubstrateBlock<T> = SignedBlock<BlockT<<T as System>::Header, <T as System>::Extrinsic>>;

/// Just one of those low-life not-signed types
// pub type NotSignedBlock<T> = BlockT<<T as System>::Header, <T as System>::Extrinsic>;
// TODO: `NotSignedBlock` and `ArchiveBackend` types should be generic over BlockNumber, Hash type and Extrinsic type
pub type NotSignedBlock = BlockT<
    sp_runtime::generic::Header<u32, sp_runtime::traits::BlakeTwo256>,
    sp_runtime::OpaqueExtrinsic,
>;

/// Read-Only RocksDb backed Backend Type
pub type ArchiveBackend = sc_client_db::Backend<NotSignedBlock>;

#[derive(Debug)]
pub struct Extrinsic<T: Substrate + Send + Sync> {
    inner: GenericExtrinsic,
    index: usize,
    hash: T::Hash,
    block_num: u32
}

impl<T> Extrinsic<T>
where
    T: Substrate + Send + Sync
{
    pub fn new(extrinsic: GenericExtrinsic, hash: T::Hash, index: usize, block_num: u32) -> Self {
        Self { inner: extrinsic, hash, index, block_num}
    }

    pub fn inner(&self) -> &GenericExtrinsic {
        &self.inner
    }

    pub fn signature(&self) -> Option<&GenericSignature> {
        self.inner.signature()
    }

    pub fn call(&self) -> &GenericCall {
        self.inner.call()
    }

    pub fn ext_module(&self) -> &str {
        self.inner.ext_module()
    }

    pub fn ext_call(&self) -> &str {
        self.inner.ext_call()
    }

    pub fn args(&self) -> &[ExtrinsicArgument] {
        self.inner.args()
    }

    pub fn hash(&self) -> &T::Hash {
        &self.hash
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }

    pub fn is_signed(&self) -> bool {
        self.inner.is_signed()
    }
}

// new types to allow implementing of traits
// NewType for Header
#[derive(Debug)]
pub struct Header<T: Substrate + Send + Sync> {
    inner: T::Header,
}

impl<T: Substrate + Send + Sync> Header<T> {
    pub fn new(header: T::Header) -> Self {
        Self { inner: header }
    }

    pub fn inner(&self) -> &T::Header {
        &self.inner
    }

    pub fn hash(&self) -> T::Hash {
        self.inner.hash()
    }
}

#[derive(Debug, Clone)]
pub struct Block<T: Substrate + Send + Sync> {
    pub inner: SubstrateBlock<T>,
    pub meta: Metadata,
    pub spec: u32,
}

// TODO: Possibly split block into extrinsics / digest / etc so that it can be sent in seperate parts to decode threads
impl<T> Block<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(block: SubstrateBlock<T>, meta: Metadata, spec: u32) -> Self {
        Self { inner: block, meta, spec }
    }

    pub fn inner(&self) -> &SubstrateBlock<T> {
        &self.inner
    }
}

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<T>
where
    T: Substrate + Send + Sync
{
    inner: Vec<Block<T>>,
}

impl<T> BatchBlock<T>
where
    T: Substrate + Send + Sync
{
    pub fn new(blocks: Vec<Block<T>>) -> Self {
        Self { inner: blocks }
    }

    pub fn inner(&self) -> &Vec<Block<T>> {
        &self.inner
    }
}


/// newType for Storage Data
#[derive(Debug)]
pub struct Storage<T: Substrate + Send + Sync> {
    data: StorageData,
    hash: T::Hash,
}

impl<T> Storage<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(data: StorageData, hash: T::Hash) -> Self {
        Self {
            data,
            hash,
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
pub struct BatchStorage<T: Substrate + Send + Sync> {
    inner: Vec<Storage<T>>,
}

impl<T> BatchStorage<T>
where
    T: Substrate + Send + Sync,
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
pub struct Event<T>
where
    T: Substrate + Send + Sync
{
    change_set: StorageChangeSet<T::Hash>,
}

impl<T> Event<T>
where
    T: Substrate + Send + Sync
{
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

/// Raw Extrinsic that can be sent between actors
/// before it's decoded into a `Extrinsic` type
/// this type is not sent to the database so it is not part of 'Data' enum
#[derive(Debug)]
pub struct RawExtrinsic<T: Substrate + Send + Sync> {
    pub inner: Vec<u8>,
    pub hash: T::Hash,
    pub spec: u32,
    pub meta: Metadata,
    pub index: usize,
    pub block_num: u32
}

impl<T> From<&Block<T>> for Vec<RawExtrinsic<T>>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    fn from(block: &Block<T>) -> Vec<RawExtrinsic<T>> {
        let block = block.clone();
        let hash = block.inner.block.header.hash();
        let spec = block.spec;
        let meta = block.meta.clone();
        let num = block.inner.block.header.number();
        block
            .inner()
            .block
            .extrinsics
            .iter()
            .enumerate()
            .map(move |(i, e)| RawExtrinsic {
                inner: e.encode(),
                hash,
                spec,
                meta: meta.clone(),
                index: i,
                block_num: (*num).into()
            })
            .collect()
    }
}

impl<T> From<BatchBlock<T>> for Vec<Vec<RawExtrinsic<T>>>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    fn from(batch_block: BatchBlock<T>) -> Vec<Vec<RawExtrinsic<T>>> {
        batch_block.inner().iter().map(|b| b.into()).collect()
    }
}
