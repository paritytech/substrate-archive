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

use log::{warn, error};
use failure::Error;
// use substrate_archive::prelude::*;
use serde::{Deserialize, Serialize};

use substrate_archive::{
    Archive, System, Module, DecodeExtrinsic,
    Extrinsic as ArchiveExtrinsic, ExtractExtrinsic,
    ExtractCall, SrmlExt, NotHandled,
    init_logger,
    srml::srml_system as system,
    Error as ArchiveError
};

use runtime_primitives::{
    AnySignature,
    OpaqueExtrinsic,
    generic
};
use polkadot_runtime::{
    Runtime as RuntimeT, Call, Address,
    ParachainsCall, ParachainsTrait, SignedExtra,
    ClaimsCall, ClaimsTrait, RegistrarCall, RegistrarTrait,
};
use polkadot_primitives::Signature;
use codec::{Encode, Decode, Input, Error as CodecError};


fn main() -> Result<(), Error> {
    // convenience log function from substrate_archive which logs to .local/share/substrate_archive
    init_logger(log::LevelFilter::Error);
    Archive::<Runtime>::new()?.run()?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtrinsicWrapper(OpaqueExtrinsic);

impl DecodeExtrinsic for ExtrinsicWrapper {
    fn decode(&self) -> Result<Box<ArchiveExtrinsic>, ArchiveError> {
        let res = ArchiveExtrinsic::<Address, CallWrapper, Signature, SignedExtra>::new(&self.0);
        if res.is_err() {
            error!("Did not decode with current Signature, trying AnySignature {:?}", res);
            Ok(Box::new(ArchiveExtrinsic::<Address, CallWrapper, AnySignature, SignedExtra>::new(&self.0)?))
        } else {
            Ok(Box::new(res?))
        }
    }
}

// need to define Encode/Decode for Call New Type
// Passthrough traits (Boilerplate)
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CallWrapper { inner: Call }
impl Encode for CallWrapper {
    fn encode(&self) -> Vec<u8> {
        self.inner.encode()
    }
}

impl Decode for CallWrapper {
    fn decode<I: Input>(input: &mut I) -> Result<Self, CodecError> {
        let decoded: Call = Decode::decode(input)?;
        Ok(CallWrapper {
            inner: decoded
        })
    }
}

// define all calls/inherents that you want tracked by the archive node
impl ExtractCall for CallWrapper {
    fn extract_call(&self) -> (Module, Box<dyn SrmlExt>) {
        match &self.inner {
            Call::Timestamp(call) => {
                (Module::Timestamp, Box::new(call.clone()))
            },
            Call::FinalityTracker(call) => {
                (Module::FinalityTracker, Box::new(call.clone()))
            },
            Call::ImOnline(call) => {
                (Module::ImOnline, Box::new(call.clone()))
            },
            Call::Babe(call) => {
                (Module::Babe, Box::new(call.clone()))
            },
            Call::Staking(call) => {
                (Module::Staking, Box::new(call.clone()))
            },
            Call::Session(call) => {
                (Module::Session, Box::new(call.clone()))
            },
            Call::Grandpa(call) => {
                (Module::Grandpa, Box::new(call.clone()))
            },
            Call::Treasury(call) => {
                (Module::Treasury, Box::new(call.clone()))
            },
            Call::Nicks(call) => {
                (Module::Nicks, Box::new(call.clone()))
            },
            Call::System(call) => {
                (Module::System, Box::new(call.clone()))
            },
            Call::Parachains(call) => {
                (Module::Custom("Parachains".into()), Box::new(ParachainsCallWrapper(call.clone())))
            },
            Call::Claims(call) => {
                (Module::Custom("Claims".into()), Box::new(ClaimsCallWrapper(call.clone())))
            },
            Call::Registrar(call) => {
                (Module::Custom("Registrar".into()), Box::new(RegistrarCallWrapper(call.clone())))
            },
            //ElectionsPhragmen
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
    T: ParachainsTrait + std::fmt::Debug
{
    fn function(&self) -> Result<(String, Vec<u8>), ArchiveError> {
        match &self.0 {
            ParachainsCall::set_heads(heads) => {
                Ok(( "set_heads".into(), vec![heads.encode()].encode() ))
            },
            __phantom_item => { // marker
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
    T: ClaimsTrait + std::fmt::Debug
{
    fn function(&self) -> Result<(String, Vec<u8>), ArchiveError> {
        match &self.0 {
            ClaimsCall::claim(account, signature) => {
                Ok(("claim".into(), vec![account.encode(), signature.encode()].encode()))
            },
            __phantom_item => { // marker
                warn!("hit phantom item");
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

#[derive(Debug)]
pub struct RegistrarCallWrapper<T: RegistrarTrait>(RegistrarCall<T>);

impl<T> SrmlExt for RegistrarCallWrapper<T>
where
    T: RegistrarTrait + std::fmt::Debug
{

    fn function(&self) -> Result<(String, Vec<u8>), ArchiveError> {
        match &self.0 {
            RegistrarCall::register_para(id, info, code, initial_head_data) => {
                Ok((
                    "register_para".into(),
                    vec![id.encode(),
                         info.encode(),
                         code.encode(),
                         initial_head_data.encode()
                    ].encode()
                ))
            },
            RegistrarCall::deregister_para(id) => {
                Ok((
                    "deregister_para".into(),
                    vec![id.encode()].encode()
                ))
            },
            RegistrarCall::set_thread_count(count) => {
                Ok((
                    "set_thread_count".into(),
                    vec![count.encode()].encode()
                ))
            },
            RegistrarCall::register_parathread(code, initial_head_data) => {
                Ok((
                    "register_parathread".into(),
                    vec![code.encode(), initial_head_data.encode()].encode()
                ))
            },
            RegistrarCall::select_parathread(id, collator, head_hash) => {
                Ok(("select_parathread".into(), vec![id.encode(), collator.encode(), head_hash.encode()].encode()
                ))
            },
            RegistrarCall::deregister_parathread() => {
                Ok(("deregister_parathread".into(), Vec::new()))
            },
            RegistrarCall::swap(other) => {
                Ok(("swap".into(), vec![other.encode()].encode()))
            },
            __phantom_item => { // marker
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
    type Extrinsic = ExtrinsicWrapper;
    type Signature = Signature;
    type Address = Address;
    type Index = <RuntimeT as system::Trait>::Index;
    type BlockNumber = <RuntimeT as system::Trait>::BlockNumber;
    type Hash = <RuntimeT as system::Trait>::Hash;
    type Hashing = <RuntimeT as system::Trait>::Hashing;
    type AccountId = <RuntimeT as system::Trait>::AccountId;
    type Header = <RuntimeT as system::Trait>::Header;
    type Event = <RuntimeT as system::Trait>::Event;
    type SignedExtra = polkadot_runtime::SignedExtra;
}
