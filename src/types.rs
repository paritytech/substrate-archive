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

use substrate_primitives::storage::StorageChangeSet;
use serde::de::DeserializeOwned;
use codec::{Encode, Decode};
use chrono::{DateTime, Utc, TimeZone};
use substrate_primitives::storage::StorageData;
use runtime_support::Parameter;
use runtime_primitives::{
    OpaqueExtrinsic,
    AnySignature,
    generic::{
        Block as BlockT,
        SignedBlock
    },
    traits::{
        Bounded,
        CheckEqual,
        Hash,
        Header as HeaderTrait,
        MaybeDisplay,
        MaybeSerializeDeserialize,
        MaybeSerialize,
        Member,
        SignedExtension,
        SimpleArithmetic,
        SimpleBitOps,
        StaticLookup,
    },
};

use crate::{error::Error, srml_ext::SrmlExt, extrinsics::Extrinsic};
use self::storage::StorageKeyType;

/// Format for describing accounts
pub type Address<T> = <<T as System>::Lookup as StaticLookup>::Source;
pub type BasicExtrinsic<T>
    = Extrinsic<Address<T>, <T as System>::Call, AnySignature, <T as System>::SignedExtra>;
/// A block with OpaqueExtrinsic as extrinsic type
pub type SubstrateBlock<T> = SignedBlock<BlockT<<T as System>::Header, OpaqueExtrinsic>>;

// pub type BlockNumber<T> = NumberOrHex<<T as System>::BlockNumber>;

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
    Offences,
    Parachains,
    RandomnessCollectiveFlip,
    ScoredPool,
    Session,
    Staking,
    Sudo,
    Support,
    // System,
    Timestamp,
    TransactionPayment,
    Treasury,
    Utility,
    Custom(String), // modules that are not defined within substrate
    NotHandled,
}


pub trait ExtractCall {
    /// module the call is from, IE Timestamp, FinalityTracker
    fn extract_call(&self) -> (Module, Box<dyn SrmlExt>);
}

// TODO: Consider removing this trait and directly using srml_system::Trait
// Right now this acts as some sort of Shim, in case we need any traits that srml_system::Trait does not specify
// which can be easily crafted in the type-specific (PolkadotArchive) portion of the code
// Issue is with getting the block number from possible unsigned values that Postgres does not support
// but using Trait is better
/// The subset of the `srml_system::Trait` that a client must implement.
pub trait System: Send + Sync + 'static + std::fmt::Debug {

    /// The Call type
    /// Should implement `ExtractCall` to put call data in a more database-friendly format
    type Call: Encode
        + Decode
        + PartialEq
        + Eq
        + Clone
        + std::fmt::Debug
        + ExtractCall;

    /// Account index (aka nonce) type. This stores the number of previous
    /// transactions associated with a sender account.
    type Index: Parameter
        + Member
        + MaybeSerialize
        + std::fmt::Debug
        + Default
        + MaybeDisplay
        + SimpleArithmetic
        + Copy;

    /// The block number type used by the runtime.
    type BlockNumber: Parameter
        + Member
        + MaybeSerializeDeserialize
        + std::fmt::Debug
        + MaybeDisplay
        + SimpleArithmetic
        + Default
        + Bounded
        + Copy
        + std::hash::Hash
        + Into<i64>;

    /// The output of the `Hashing` function.
    type Hash: Parameter
        + Member
        + MaybeSerializeDeserialize
        + std::fmt::Debug
        + MaybeDisplay
        + SimpleBitOps
        + Default
        + Copy
        + CheckEqual
        + std::hash::Hash
        + AsRef<[u8]>
        + AsMut<[u8]>
        + std::marker::Unpin;

    /// The hashing system (algorithm) being used in the runtime (e.g. Blake2).
    type Hashing: Hash<Output = Self::Hash>;

    /// The user account identifier type for the runtime.
    type AccountId: Parameter
        + Member
        + MaybeSerializeDeserialize
        + std::fmt::Debug
        + MaybeDisplay
        + Ord
        + Default;

    /// Converting trait to take a source type and convert to `AccountId`.
    ///
    /// Used to define the type and conversion mechanism for referencing
    /// accounts in transactions. It's perfectly reasonable for this to be an
    /// identity conversion (with the source type being `AccountId`), but other
    /// modules (e.g. Indices module) may provide more functional/efficient
    /// alternatives.
    type Lookup: StaticLookup<Target = Self::AccountId>;

    /// The block header.
    type Header: Parameter
        + HeaderTrait<Number = Self::BlockNumber, Hash = Self::Hash>
        + DeserializeOwned
        + Clone
        + Unpin;

    /// The aggregated event type of the runtime.
    type Event: Parameter + Member;

    /// The `SignedExtension` to the basic transaction logic.
    type SignedExtra: SignedExtension;
}
