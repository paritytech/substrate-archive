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

use substrate_subxt::srml::system::System;
use runtime_primitives::OpaqueExtrinsic as UncheckedExtrinsic;
use runtime_primitives::generic::{Block as BlockT, SignedBlock};
use substrate_primitives::storage::StorageChangeSet;
use substrate_rpc_primitives::number::NumberOrHex;


pub type Block<T> = SignedBlock<BlockT<<T as System>::Header, UncheckedExtrinsic>>;
pub type BlockNumber<T> = NumberOrHex<<T as System>::BlockNumber>;

#[derive(Debug, PartialEq, Eq)]
pub enum Payload<T: System> {
    FinalizedHead(T::Header),
    Header(T::Header),
    BlockNumber(T::BlockNumber),
    Hash(T::Hash),
    Block(Block<T>),
    Event(StorageChangeSet<T::Hash>),
}

/// Sent from Substrate API to be committed into the Database
#[derive(Debug, PartialEq, Eq)]
pub struct Data<T: System> {
    pub payload: Payload<T>
}
