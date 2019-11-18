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

pub mod storage;
mod traits;

use substrate_primitives::storage::StorageChangeSet;
use codec::Decode;
use chrono::{DateTime, Utc, TimeZone};
use substrate_primitives::storage::StorageData;
use runtime_primitives::{
    MultiSignature, AnySignature, generic::{Block as BlockT, SignedBlock},
};

pub use self::traits::{ExtractCall, DecodeExtrinsic, System, ExtrinsicExt};

use crate::error::Error;
use self::storage::StorageKeyType;

// /// Format for describing accounts

// pub type BasicExtrinsic<T>
//   = Extrinsic<<T as System>::Address, <T as System>::Call, <T as System>::Signature, <T as System>::SignedExtra>;
// /// A block with OpaqueExtrinsic as extrinsic type
pub type SubstrateBlock<T> = SignedBlock<BlockT<<T as System>::Header, <T as System>::Extrinsic>>;

/// Sent from Substrate API to be committed into the Database
#[derive(Debug, PartialEq, Eq)]
pub enum Data<T: System> {
    Header(Header<T>),
    FinalizedHead(Header<T>),
    Block(Block<T>),
    BatchBlock(BatchBlock<T>),
    BatchStorage(BatchStorage<T>), // include callback on storage types for exact diesel::call
    Storage(Storage<T>),
    Event(Event<T>),
    SyncProgress(usize),
}
#[derive(Debug)]
pub enum DbExtrinsic<T: System> {
    Signed(SignedExtrinsic),
    NotSigned(Box<dyn ExtractCall>),

}

pub struct SignedExtrinsic {
    pub signature: Vec<u8>,
    pub address: Vec<u8>,
    pub extra: Vec<u8>,
    pub call: Box<dyn ExtractCall>,
}

// new types to allow implementing of traits
#[derive(Debug, PartialEq, Eq)]
pub struct Header<T: System> {
    inner: T::Header
}

impl<T: System> Header<T> {

    pub fn new(header: T::Header) -> Self {
        Self {
            inner: header
        }
    }

    pub fn inner(&self) -> &T::Header {
        &self.inner
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Block<T: System> {
    inner: SubstrateBlock<T>
}

impl<T: System> Block<T> {

    pub fn new(block: SubstrateBlock<T>) -> Self {
        Self {
            inner: block
        }
    }

    pub fn inner(&self) -> &SubstrateBlock<T> {
        &self.inner
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BatchBlock<T: System> {
    inner: Vec<SubstrateBlock<T>>
}

impl<T: System> BatchBlock<T> {

    pub fn new(blocks: Vec<SubstrateBlock<T>>) -> Self {
        Self { inner: blocks }
    }

    pub fn inner(&self) -> &Vec<SubstrateBlock<T>> {
        &self.inner
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Storage<T: System> {
    data: StorageData,
    key_type: StorageKeyType,
    hash: T::Hash // TODO use T:Hash
}

impl<T> Storage<T>
where
    T: System
{

    pub fn new(data: StorageData, key_type: StorageKeyType, hash: T::Hash) -> Self {
        Self { data, key_type, hash }
    }

    pub fn data(&self) -> &StorageData {
        &self.data
    }

    pub fn key_type(&self) -> &StorageKeyType {
        &self.key_type
    }

    pub fn hash(&self) -> &T::Hash {
        &self.hash
    }

    pub fn get_timestamp(&self) -> Result<DateTime<Utc>, Error> {
        // TODO: check if storage key type is actually from the timestamp module
        let unix_time: i64 = Decode::decode(&mut self.data().0.as_slice())?;
        Ok(Utc.timestamp_millis(unix_time)) // panics if time is incorrect
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BatchStorage<T: System> {
    inner: Vec<Storage<T>>,
}

impl<T> BatchStorage<T>
where
    T: System
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

#[derive(Debug, PartialEq, Eq)]
pub struct Event<T: System> {
    change_set: StorageChangeSet<T::Hash>
}

impl<T: System> Event<T> {

    pub fn new(change_set: StorageChangeSet<T::Hash>) -> Self {
        Self { change_set }
    }

    pub fn change_set(&self) -> &StorageChangeSet<T::Hash> {
        &self.change_set
    }
}

#[derive(Debug, PartialEq, Eq, Clone, derive_more::Display)]
pub enum Module {
    Assets,
    Aura,
    AuthorityDiscovery,
    Authorship,
    Babe,
    Balances,
    Collective,
    Contracts,
    Democracy,
    Elections,
    ElectionsPhragmen,
    Executive,
    FinalityTracker,
    GenericAsset,
    Grandpa,
    ImOnline,
    Membership,
    Metadata,
    Nicks,
    Offences,
    Parachains,
    RandomnessCollectiveFlip,
    ScoredPool,
    Session,
    Staking,
    Sudo,
    Support,
    System,
    Timestamp,
    TransactionPayment,
    Treasury,
    Utility,
    Custom(String), // modules that are not defined within substrate
    NotHandled,
}
