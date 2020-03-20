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

//! Default Runtime for tests


// use codec::{Decode, Encode, Error as CodecError, Input};
#[cfg(test)]
use node_runtime::{Call, Runtime as RuntimeT, SignedExtra};
use frame_system::Trait as System;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Runtime;
impl System for Runtime {
    type Call = CallWrapper;
    type Index = <RuntimeT as System>::Index;
    type BlockNumber = <RuntimeT as System>::BlockNumber;
    type Hash = <RuntimeT as System>::Hash;
    type Hashing = <RuntimeT as System>::Hashing;
    type AccountId = <RuntimeT as System>::AccountId;
    type Lookup = <RuntimeT as System>::Lookup;
    type Header = <RuntimeT as System>::Header;
    type Event = <RuntimeT as System>::Event;
}
