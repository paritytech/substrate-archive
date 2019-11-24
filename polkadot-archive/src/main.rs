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
use serde_json::{json, Value};
use substrate_archive::{
    Archive, System, Module, RawExtrinsic,
    OldExtrinsic, ToDatabaseExtrinsic,
    ExtractCall, PaintExt, NotHandled,
    init_logger,
    paint::paint_system as system,
    // paint::paint_sudo::{Trait as SudoTrait, Call as SudoCall},
    Error as ArchiveError
};
use runtime_primitives::{
    AnySignature,
    OpaqueExtrinsic,
    // generic::UncheckedExtrinsic,
};
use polkadot_runtime::{
    Runtime as RuntimeT, Call, Address,
    ParachainsCall, ParachainsTrait, SignedExtra,
    ClaimsCall, ClaimsTrait, RegistrarCall, RegistrarTrait,
};

use polkadot_primitives::Signature;
use codec::{Encode, Decode, Input, Error as CodecError};

use std::fmt::Debug;

fn main() -> Result<(), Error> {
    // convenience log function from substrate_archive which logs to .local/share/substrate_archive
    init_logger(log::LevelFilter::Error);
    Archive::<Runtime>::new()?.run()?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtrinsicWrapper(OpaqueExtrinsic);
impl ToDatabaseExtrinsic for ExtrinsicWrapper {
    fn to_database(&self) -> Result<RawExtrinsic, ArchiveError> {
        let opaque = &self.0;

        let res: Result<OldExtrinsic::<Address, CallWrapper, Signature, SignedExtra>, _>
            = Decode::decode(&mut opaque.encode().as_slice());
        if res.is_err() {
            warn!("Did not decode with current Signature, trying AnySignature {:?}", res);
            let ext: OldExtrinsic::<Address, CallWrapper, AnySignature, SignedExtra>
                = Decode::decode(&mut opaque.encode().as_slice())?;
            Ok(ext.into())
        } else {
            Ok(res?.into())
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
    fn extract_call(&self) -> (Module, Box<dyn PaintExt>) {
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
            Call::Balances(call) => {
                (Module::Balances, Box::new(call.clone()))
            },
            Call::ElectionsPhragmen(call) => {
                (Module::ElectionsPhragmen, Box::new(call.clone()))
            },
            Call::Staking(call) => {
                (Module::Staking, Box::new(call.clone()))
            },
            Call::Sudo(call) => {
                (Module::Sudo, Box::new(call.clone()))
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
            c @ _ => {
                warn!("Call Not Handled: {:?}", c);
                (Module::NotHandled, Box::new(NotHandled))
            }
        }
    }
}

// Sudo module should be implemented manually because it wraps other calls
// this enables the wrapped calls to also be decoded
// if this is not done, Sudo will still be committed to the database but with the entire Call SCALE-encoded
/*
/////////////////
// Sudo Module //
/////////////////
#[derive(Debug, Clone, PartialEq)]
pub struct SudoCallWrapper<T: SudoTrait>(SudoCall<T>);

impl<T> PaintExt for SudoCallWrapper<T>
where
    T: SudoTrait + Debug
{
    fn function(&self) -> Result<(String, Value), ArchiveError> {
        match &self.0 {
            SudoCall::sudo(proposal) => {
                let sudo: CallWrapper = Decode::decode(&mut proposal)?;
                let (module, call) = sudo.extract_call();
                let (fn_name, params) = call.function()?;
                let val = json!([
                    {
                        "proposal": {
                            "module": module,
                            "function": fn_name,
                            "parameters": params
                        }
                    }
                ]);
                Ok(("proposal".into(), val))
            },
            SudoCall::set_key(new) => {
                let val = json!([
                    { "new": new.encode(), "encoded": true }
                ]);
                Ok(("set_key".into(), val))
            },
            &__phantom_item => {
                Ok(("__phantom".into(), json!({}) ))
            }
        }
    }
}
*/
////////////////////
// Custom Modules //
////////////////////
#[derive(Debug, Clone, PartialEq)]
pub struct ParachainsCallWrapper<T: ParachainsTrait>(ParachainsCall<T>);

impl<T> PaintExt for ParachainsCallWrapper<T>
where
    T: ParachainsTrait + Debug
{
    fn function(&self) -> Result<(String, Value), ArchiveError> {
        match &self.0 {
            ParachainsCall::set_heads(heads) => {
                let val = json!([
                    { "heads": heads.encode(), "encoded": true }
                ]);
                Ok(( "set_heads".into(), val ))
            },
            __phantom_item => { // marker
                warn!("hit phantom item");
                Ok(("".into(), json!({}) ))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimsCallWrapper<T: ClaimsTrait>(ClaimsCall<T>);

impl<T> PaintExt for ClaimsCallWrapper<T>
where
    T: ClaimsTrait + Debug
{
    fn function(&self) -> Result<(String, Value), ArchiveError> {
        match &self.0 {
            ClaimsCall::claim(account, signature) => {
                let val = json!([
                    { "account": account },
                    { "signature": signature.encode(), "encoded": true } // TODO Implements Serialize on Master!
                ]);
                Ok(("claim".into(), val))
            },
            __phantom_item => { // marker
                warn!("hit phantom item");
                Ok(("".into(), json!({}) ))
            }
        }
    }
}

#[derive(Debug)]
pub struct RegistrarCallWrapper<T: RegistrarTrait>(RegistrarCall<T>);

impl<T> PaintExt for RegistrarCallWrapper<T>
where
    T: RegistrarTrait + Debug
{

    fn function(&self) -> Result<(String, Value), ArchiveError> {
        match &self.0 {
            RegistrarCall::register_para(id, info, code, initial_head_data) => {
                let val = json!([
                    { "id": id },
                    { "info": info.encode(), "encoded": true }, // TODO implements Serialize on next release
                    { "code": code },
                    { "initial_head_data": initial_head_data }
                ]);
                Ok(("register_para".into(), val))
            },
            RegistrarCall::deregister_para(id) => {
                let val = json!([
                    { "id": id }
                ]);
                Ok(("deregister_para".into(), val ))
            },
            RegistrarCall::set_thread_count(count) => {
                let val = json!([
                    { "count": count }
                ]);
                Ok(( "set_thread_count".into(), val))
            },
            RegistrarCall::register_parathread(code, initial_head_data) => {
                let val = json!([
                    { "code": code },
                    { "initial_head_data": initial_head_data }
                ]);
                Ok(( "register_parathread".into(), val ))
            },
            RegistrarCall::select_parathread(id, collator, head_hash) => {
                let val = json!([
                    { "id": id },
                    { "collator": collator },
                    { "head_hash": head_hash }
                ]);
                Ok(("select_parathread".into(), val ))
            },
            RegistrarCall::deregister_parathread() => {
                Ok(("deregister_parathread".into(), json!({}) ))
            },
            RegistrarCall::swap(other) => {
                let val = json!([
                    { "other": other }
                ]);
                Ok(("swap".into(), val))
            },
            __phantom_item => { // marker
                warn!("hit phantom item");
                Ok(("".into(), json!({}) ))
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
