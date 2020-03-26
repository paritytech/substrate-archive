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

use runtime_primitives::{generic::{Block as BlockT, SignedBlock}, traits::Header as _};
use substrate_primitives::storage::{StorageChangeSet, StorageData};
use subxt::{ system::System, balances::Balances};

/// Consolidation of substrate traits representing fundamental types
pub trait Substrate: System + Balances {}

/// A generic substrate block
pub type SubstrateBlock<T: Substrate> = SignedBlock<BlockT<<T as System>::Header, <T as System>::Extrinsic>>;

pub enum BatchData<T: Substrate> {
    BatchBlock(BatchBlock<T>),
    BatchStorage(BatchStorage<T>),
}

impl<T> BatchData<T> where T: Substrate {
    pub fn hashes(&self) -> &[&T::Hash] {
        match self {
            BatchData::BatchBlock(b) => {
                b.inner().iter().map(|b| &b.block.header.hash()).collect::<Vec<&T::Hash>>().as_slice()
            },
            BatchData::BatchStorage(s) => {
                s.inner().iter().map(|s| s.hash()).collect::<Vec<&T::Hash>>().as_slice()
            }
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

impl<T> Data<T> where T: Substrate {
    /// get the hash for a data item
    pub fn hash(&self) -> &T::Hash {
        match self {
            Data::Header(h) => {
                h.hash()
            },
            Data::Block(b) => {
                &b.inner.block.header.hash()
            },
            Data::Storage(s) => {
                s.hash() 
            },
            Data::Event(e) => {
                &e.hash()
            }
        }
    }
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

/// NewType for Block
#[derive(Debug)]
pub struct Block<T: Substrate> {
    inner: SubstrateBlock<T>,
}

impl<T: Substrate> Block<T> {
    pub fn new(block: SubstrateBlock<T>) -> Self {
        Self { inner: block }
    }

    pub fn inner(&self) -> &SubstrateBlock<T> {
        &self.inner
    }
}

/// NewType for committing many blocks to the database at once
#[derive(Debug)]
pub struct BatchBlock<T: Substrate> {
    inner: Vec<SubstrateBlock<T>>,
}

impl<T: Substrate> BatchBlock<T> {
    pub fn new(blocks: Vec<SubstrateBlock<T>>) -> Self {
        Self { inner: blocks }
    }

    pub fn inner(&self) -> &Vec<SubstrateBlock<T>> {
        &self.inner
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
