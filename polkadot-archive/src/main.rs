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

use codec::{Decode, Encode, Error as CodecError, Input};
use failure::Error;
use log::warn;
use polkadot_runtime::{
    Call, ClaimsCall, ClaimsTrait, ParachainsCall, ParachainsTrait, Runtime as RuntimeT,
};
use substrate_archive::{
    srml::srml_system as system, Archive, Error as ArchiveError, ExtractCall, Module, NotHandled,
    SrmlExt, System,
};

fn main() -> Result<(), Error> {
    Archive::<Runtime>::new()?.run()?;
    Ok(())
}

// need to define Encode/Decode for Call New Type

// Passthrough traits (Boilerplate)
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
    fn extract_call(&self) -> (Module, Box<dyn SrmlExt>) {
        match &self.inner {
            Call::Timestamp(call) => (Module::Timestamp, Box::new(call.clone())),
            Call::FinalityTracker(call) => (Module::FinalityTracker, Box::new(call.clone())),
            Call::ImOnline(call) => (Module::ImOnline, Box::new(call.clone())),
            Call::Babe(call) => (Module::Babe, Box::new(call.clone())),
            Call::Staking(call) => (Module::Staking, Box::new(call.clone())),
            Call::Session(call) => (Module::Session, Box::new(call.clone())),
            Call::Grandpa(call) => (Module::Grandpa, Box::new(call.clone())),
            Call::Treasury(call) => (Module::Treasury, Box::new(call.clone())),
            Call::Parachains(call) => (
                Module::Custom("Parachains".into()),
                Box::new(ParachainsCallWrapper(call.clone())),
            ),
            Call::Claims(call) => (
                Module::Custom("Claims".into()),
                Box::new(ClaimsCallWrapper(call.clone())),
            ),
            c @ _ => {
                warn!("Call Not Handled: {:?}", c);
                (Module::NotHandled, Box::new(NotHandled))
            }
        }
    }
}

////////////////////
// Custom Modules //
////////////////////
#[derive(Debug, Clone, PartialEq)]
pub struct ParachainsCallWrapper<T: ParachainsTrait>(ParachainsCall<T>);

impl<T> SrmlExt for ParachainsCallWrapper<T>
where
    T: ParachainsTrait + std::fmt::Debug,
{
    fn function(&self) -> Result<(String, Vec<u8>), ArchiveError> {
        match &self.0 {
            ParachainsCall::set_heads(heads) => {
                Ok(("set_heads".into(), vec![heads.encode()].encode()))
            }
            __phantom_item => {
                // marker
                warn!("hit phantom item");
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimsCallWrapper<T: ClaimsTrait>(ClaimsCall<T>);

impl<T> SrmlExt for ClaimsCallWrapper<T>
where
    T: ClaimsTrait + std::fmt::Debug,
{
    fn function(&self) -> Result<(String, Vec<u8>), ArchiveError> {
        match &self.0 {
            ClaimsCall::claim(account, signature) => Ok((
                "claim".into(),
                vec![account.encode(), signature.encode()].encode(),
            )),
            __phantom_item => {
                // marker
                warn!("hit phantom item");
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Runtime;
impl System for Runtime {
    type Call = CallWrapper;
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
