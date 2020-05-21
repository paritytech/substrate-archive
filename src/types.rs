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
use std::marker::PhantomData;
// use sp_core::storage::{StorageChangeSet, StorageData};
use sp_runtime::{
    generic::{Block as BlockT, SignedBlock},
    traits::{Block as _, Header as _},
};
use sp_storage::{StorageData, StorageKey};
use subxt::system::System;

pub use self::traits::Substrate;

/// A generic substrate block
pub type SubstrateBlock<T> = SignedBlock<BlockT<<T as System>::Header, <T as System>::Extrinsic>>;

/// Just one of those low-life not-signed types
pub type NotSignedBlock = BlockT<
    sp_runtime::generic::Header<u32, sp_runtime::traits::BlakeTwo256>,
    sp_runtime::OpaqueExtrinsic,
>;

/// Read-Only RocksDb backed Backend Type
pub type ArchiveBackend = sc_client_db::Backend<NotSignedBlock>;

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
pub struct Extrinsic<T: Substrate + Send + Sync> {
    pub hash: Vec<u8>,
    /// The SCALE-encoded extrinsic
    pub inner: Vec<u8>,
    /// Spec that the extrinsic is from
    pub spec: u32,
    pub index: u32,
    _marker: PhantomData<T>,
}

impl<T> Extrinsic<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(ext: &T::Extrinsic, hash: T::Hash, index: u32, spec: u32) -> Self {
        Self {
            hash: hash.as_ref().to_vec(),
            inner: ext.encode(),
            _marker: PhantomData,
            index,
            spec,
        }
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

    pub fn inner(&self) -> &SubstrateBlock<T> {
        &self.inner
    }

    pub fn spec(&self) -> u32 {
        self.spec
    }

    pub fn hash(&self) -> T::Hash {
        self.inner().block.header.hash()
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

/// newType for Storage Data
#[derive(Clone, Debug)]
pub struct Storage<T: Substrate + Send + Sync> {
    hash: T::Hash,
    block_num: u32,
    full_storage: bool,
    key: StorageKey,
    data: Option<StorageData>,
}

impl<T> Storage<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(
        hash: T::Hash,
        block_num: u32,
        full_storage: bool,
        key: StorageKey,
        data: Option<StorageData>,
    ) -> Self {
        Self {
            block_num,
            hash,
            full_storage,
            key,
            data,
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

    pub fn key(&self) -> &StorageKey {
        &self.key
    }

    pub fn data(&self) -> Option<&StorageData> {
        self.data.as_ref()
    }
}

impl<T> From<&Block<T>> for Vec<Extrinsic<T>>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    fn from(block: &Block<T>) -> Vec<Extrinsic<T>> {
        let spec = block.spec;
        let hash = block.hash();
        block
            .inner()
            .block
            .extrinsics
            .iter()
            .enumerate()
            .map(move |(i, e)| Extrinsic::new(e, hash, i as u32, spec))
            .collect::<Vec<Extrinsic<T>>>()
    }
}

impl<T> From<BatchBlock<T>> for Vec<Extrinsic<T>>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    fn from(batch_block: BatchBlock<T>) -> Vec<Extrinsic<T>> {
        batch_block
            .inner()
            .iter()
            .map(|b| b.into())
            .collect::<Vec<Vec<Extrinsic<T>>>>()
            .into_iter()
            .flatten()
            .collect()
    }
}
