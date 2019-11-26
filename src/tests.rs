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
use crate::{ExtractCall, Module, NotHandled, SrmlExt, System};
use codec::{Decode, Encode, Error as CodecError, Input};
#[cfg(test)]
use node_runtime::{Call, Runtime as RuntimeT, SignedExtra};
use srml_system::Trait;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Runtime;
impl System for Runtime {
    type Call = CallWrapper;
    type Index = <RuntimeT as Trait>::Index;
    type BlockNumber = <RuntimeT as Trait>::BlockNumber;
    type Hash = <RuntimeT as Trait>::Hash;
    type Hashing = <RuntimeT as Trait>::Hashing;
    type AccountId = <RuntimeT as Trait>::AccountId;
    type Lookup = <RuntimeT as Trait>::Lookup;
    type Header = <RuntimeT as Trait>::Header;
    type Event = <RuntimeT as Trait>::Event;
    type SignedExtra = SignedExtra;
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CallWrapper {
    inner: Call,
}
impl Encode for CallWrapper {
    fn encode(&self) -> Vec<u8> {
        self.inner.encode()
    }
}

impl Decode for CallWrapper {
    fn decode<I: Input>(input: &mut I) -> Result<Self, CodecError> {
        let decoded: Call = Decode::decode(input)?;
        Ok(CallWrapper { inner: decoded })
    }
}

// define all calls/inherents that you want tracked by the archive node
impl ExtractCall for CallWrapper {
    fn extract_call(&self) -> (Module, &dyn SrmlExt) {
        match &self.inner {
            Call::Timestamp(call) => (Module::Timestamp, call),
            Call::FinalityTracker(call) => (Module::FinalityTracker, call),
            _ => {
                println!("Unsupported Module");
                (Module::NotHandled, &NotHandled)
            }
        }
    }
}
