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

use substrate_subxt::srml::{balances::Balances, contracts::Contracts, system::System};
use sr_primitives::OpaqueExtrinsic as UncheckedExtrinsic;
use sr_primitives::generic::{Era, SignedBlock /* Header */};
use sr_primitives::traits::{StaticLookup, Block as BlockT/* BlakeTwo256 */ };
use node_primitives::{Hash, Header, Block}; // Block == Block<Header, UncheckedExtrinsic>

/// The type of data this entry contains
#[derive(Debug, PartialEq, Eq)]
pub enum DataEntryType {
    FinalizedBlock,
    NewHead
}

#[derive(Debug, PartialEq, Eq)]
pub enum Payload<T: System + BlockT> {
    Extrinsic(T::Extrinsic),
    Header(<T as BlockT>::Header),
    BlockNumber(T::BlockNumber),
    None,
}

/// Sent from Substrate API to be committed into the Database
#[derive(Debug, PartialEq, Eq)]
pub struct Data<T: System + BlockT> {
    pub info: DataEntryType,
    pub payload: Payload<T>
}

