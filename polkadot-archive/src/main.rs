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

//! Specify types for a specific Blockchain -- E.G Kusama/Polkadot and run the archive node with these types

use failure::Error;
use substrate_archive::{ System };
use polkadot_runtime::Runtime as RuntimeT;

fn main() -> Result<(), Error> {
    substrate_archive::run::<Runtime>().map_err(Into::into)
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Runtime;
impl System for Runtime {
    type Call = <RuntimeT as system::Trait>::Call;
    type Index = <RuntimeT as system::Trait>::Index;
    type BlockNumber = <RuntimeT as system::Trait>::BlockNumber;
    type Hash = <RuntimeT as system::Trait>::Hash;
    type Hashing = <RuntimeT as system::Trait>::Hashing;
    type AccountId = <RuntimeT as system::Trait>::AccountId;
    type Lookup = <RuntimeT as system::Trait>::Lookup;
    type Header = <RuntimeT as system::Trait>::Header;
    type Event = <RuntimeT as system::Trait>::Event;
    type SignedExtra = polkadot_runtime::SignedExtra;
}
