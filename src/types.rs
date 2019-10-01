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

use substrate_primitives::storage::StorageChangeSet;
use serde::de::DeserializeOwned;
use srml_system::Event;
use codec::{Encode, Decode};
use runtime_primitives::{
    OpaqueExtrinsic,
    AnySignature,
    generic::{
        UncheckedExtrinsic,
        Block as BlockT,
        SignedBlock
    },
    traits::{
        Bounded,
        CheckEqual,
        Hash,
        Header,
        MaybeDisplay,
        MaybeSerializeDebug,
        MaybeSerializeDebugButNotDeserialize,
        Member,
        SignedExtension,
        SimpleArithmetic,
        SimpleBitOps,
        StaticLookup,
    },
};

use runtime_support::Parameter;
/// Format for describing accounts
pub type Address<T> = <<T as System>::Lookup as StaticLookup>::Source;
/// Basic Extrinsic Type. Does not contain an ERA
pub type BasicExtrinsic<T> = UncheckedExtrinsic<Address<T>, <T as System>::Call, AnySignature, <T as System>::SignedExtra >;
/// A block with OpaqueExtrinsic as extrinsic type
pub type Block<T> = SignedBlock<BlockT<<T as System>::Header, OpaqueExtrinsic>>;

// pub type BlockNumber<T> = NumberOrHex<<T as System>::BlockNumber>;

/// Sent from Substrate API to be committed into the Database
#[derive(Debug, PartialEq, Eq)]
pub enum Data<T: System> {
    FinalizedHead(T::Header),
    Header(T::Header),
    // Hash(T::Hash),
    Block(Block<T>),
    Event(StorageChangeSet<T::Hash>),
}

// TODO: Consider removing this trait and directly using srml_system::Trait
// Right now this acts as some sort of Shim, in case we need any traits that srml_system::Trait does not specify
// which can be easily crafted in the type-specific (PolkadotArchive) portion of the code
// Issue is with getting the block number from possible unsigned values that Postgres does not support
// but using Trait is better
/// The subset of the `srml_system::Trait` that a client must implement.
pub trait System {

    /// The Call type
    type Call: Encode
        + Decode
        + PartialEq
        + Eq
        + Clone
        + std::fmt::Debug;

    /// Account index (aka nonce) type. This stores the number of previous
    /// transactions associated with a sender account.
    type Index: Parameter
        + Member
        + MaybeSerializeDebugButNotDeserialize
        + Default
        + MaybeDisplay
        + SimpleArithmetic
        + Copy;

    /// The block number type used by the runtime.
    type BlockNumber: Parameter
        + Member
        + MaybeSerializeDebug
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
        + MaybeSerializeDebug
        + MaybeDisplay
        + SimpleBitOps
        + Default
        + Copy
        + CheckEqual
        + std::hash::Hash
        + AsRef<[u8]>
        + AsMut<[u8]>;

    /// The hashing system (algorithm) being used in the runtime (e.g. Blake2).
    type Hashing: Hash<Output = Self::Hash>;

    /// The user account identifier type for the runtime.
    type AccountId: Parameter
        + Member
        + MaybeSerializeDebug
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
        + Header<Number = Self::BlockNumber, Hash = Self::Hash>
        + DeserializeOwned;

    /// The aggregated event type of the runtime.
    type Event: Parameter + Member;

    /// The `SignedExtension` to the basic transaction logic.
    type SignedExtra: SignedExtension;
}
