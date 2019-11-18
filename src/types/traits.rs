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

use std::fmt::Debug;

use crate::{error::Error, srml_ext::SrmlExt, extrinsics::{UncheckedExtrinsic, Extrinsic, ExtractExtrinsic}};
use super::{Module, DbExtrinsic};

use codec::{Encode, Decode};
use serde::{Serialize, de::DeserializeOwned};
use runtime_support::Parameter;
use runtime_primitives::{
    AnySignature,
    MultiSignature,
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
    }
};

pub trait ToDatabaseExtrinsic {
    fn to_database(&self) -> DbExtrinsic;
}

pub trait ExtractExtrinsic {
    fn extract_extrinsic(&self) -> Box<dyn ExtrinsicExt>;
}

pub trait ExtrinsicExt: Debug {
    type Address;
    type Extra;
    fn signature(&self) -> Option<(Self::Address, Vec<u8>, Self::Extra)>;
    fn call(&self) -> Box<dyn ExtractCall>;
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
pub trait System: Send + Sync + 'static + Debug {

    /// The Call type
    /// Should implement `ExtractCall` to put call data in a more database-friendly format
    type Call: Encode + Decode + Clone + Debug + ExtractCall; // TODO import Debug
    type Extrinsic: DecodeExtrinsic + Debug + Serialize + DeserializeOwned + Clone + Eq + PartialEq + Unpin;
    // type Block: BlockTrait + Encode + Decode + Debug;
    type Signature: Encode + Decode + Debug + GenericBytes;
    type Address: Encode + Decode + Debug;

    type Generic: GenericBytes;

    /// Account index (aka nonce) type. This stores the number of previous
    /// transactions associated with a sender account.
    type Index: Parameter
        + Member
        + MaybeSerialize
        + Debug
        + Default
        + MaybeDisplay
        + SimpleArithmetic
        + Copy;

    /// The block number type used by the runtime.
    type BlockNumber: Parameter
        + Member
        + MaybeSerializeDeserialize
        + Debug
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
        + Debug
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
        + Debug
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
    // type Lookup: StaticLookup<Target = Self::AccountId>;

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
