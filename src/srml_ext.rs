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
//! Extensions to Srml Modules to extract data useful in a database scenario
use srml_timestamp::Call as TimestampCall;
use srml_finality_tracker::Call as FinalityCall;
use srml_sudo::Call as SudoCall;
use codec::{Encode, Decode};

use crate::types::{ System };
use crate::error::Error;

pub trait SrmlExt: std::fmt::Debug {
    fn function(&self) -> Result<(CallName, Parameters), Error>; // name of the function as a string
}

/// Name of the call
pub type CallName = String;
/// SCALE Encoded Parameters
pub type Parameters = Vec<u8>;
/// Convenience type
pub type FunctionInfo = (CallName, Parameters);
type SrmlResult<T> = Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotHandled;

impl SrmlExt for NotHandled {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        Err(Error::UnhandledCallType)
    }
}

impl<T> SrmlExt for TimestampCall<T> where T: srml_timestamp::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            TimestampCall::set(block) => {
                Ok(("set".into(), block.encode()))
            },
            _ => {
                // TODO: Error
                panic!("Match statement should never hit phantom data; qed")
            }
        }
    }
}

impl<T> SrmlExt for FinalityCall<T> where T: srml_finality_tracker::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            FinalityCall::final_hint(time) => {
                Ok(("final_hint".into(), time.encode()))
            },
            _ => {
                //TODO Error
                panic!("Match statement should never hit phantom data; qed")
            }
        }
    }
}

impl<T> SrmlExt for SudoCall<T> where T: srml_sudo::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            SudoCall::sudo(proposal) => {
                Ok(("sudo".into(), proposal.encode()))
            },
            SudoCall::set_key(source) => {
                Ok(("set_key".into(), source.encode()))
            },
            _ => {
                panic!("Match statement never hits phantom data; qed")
                // TODO Error
            }
        }
    }
}
