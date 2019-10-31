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

// use srml_assets::Call as AssetsCall;
use srml_aura::Call as AuraCall;
use srml_timestamp::Call as TimestampCall;
use srml_finality_tracker::Call as FinalityCall;
use srml_sudo::Call as SudoCall;
use srml_babe::Call as BabeCall;
use srml_session::Call as SessionCall;
use srml_im_online::Call as ImOnlineCall;
use srml_staking::Call as StakingCall;
use srml_grandpa::Call as GrandpaCall;
use srml_treasury::Call as TreasuryCall;
// use runtime_support::dispatch::{IsSubType, Callable};
use codec::Encode;

use crate::error::Error;

pub trait SrmlExt: std::fmt::Debug {
    /// Seperates a call into it's name and parameters
    /// Parameters are SCALE encoded
    fn function(&self) -> Result<(CallName, Parameters), Error>; // name of the function as a string

}

/// Name of the function
pub type CallName = String;
/// SCALE Encoded Parameters
pub type Parameters = Vec<u8>;

// TODO: look to store parameters in something other than SCALE
// like raw bit-array
// problem is for modules that contain data other than simple u32's (ex: sudo)
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

/*
impl<T> SrmlExt for AssetsCall<T> where T: srml_assets::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            AssetsCall::balances(b) => {
                Ok(("balances".into(), b.encode()))
            }
        }
    }
}
 */

impl<T> SrmlExt for AuraCall<T> where T: srml_aura::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for BabeCall<T> where T: srml_babe::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for SessionCall<T> where T: srml_session::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            SessionCall::set_keys(keys, proof) => {
                Ok(("set_keys".into(), vec![keys.encode(), proof.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

// matching exhaustively on &__phantom_item allows the compiler to implicitly
// check making sure all Call types are covered
impl<T> SrmlExt for TimestampCall<T> where T: srml_timestamp::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            TimestampCall::set(time) => {
                Ok(("set".into(), vec![time.encode()].encode() ))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for FinalityCall<T> where T: srml_finality_tracker::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            FinalityCall::final_hint(block) => {
                Ok(("final_hint".into(), vec![block.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for ImOnlineCall<T> where T: srml_im_online::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            ImOnlineCall::heartbeat(heartbeat, signature) => {
                Ok(("im-online".into(), vec![heartbeat.encode(), signature.encode()].encode()))
            },
            &__phantom_item => {
                 Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for StakingCall<T> where T: srml_staking::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            StakingCall::bond(controller, value, payee) => {
                Ok(("bond".into(), vec![controller.encode(), value.encode(), payee.encode()].encode()))
            },
            StakingCall::bond_extra(max_additional) => {
                Ok(("bond_extra".into(), vec![max_additional.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for GrandpaCall<T> where T: srml_grandpa::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            GrandpaCall::report_misbehavior(report) => {
                Ok(("report_misbehavior".into(), vec![report.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for SudoCall<T> where T: srml_sudo::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            SudoCall::sudo(proposal) => {
                Ok(("sudo".into(), vec![proposal.encode()].encode()))
            },
            SudoCall::set_key(source) => {
                Ok(("set_key".into(), vec![source.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for TreasuryCall<T> where T: srml_treasury::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            TreasuryCall::propose_spend(value, beneficiary) => {
                Ok(("propose_spend".into(), vec![value.encode(), beneficiary.encode()].encode()))
            },
            TreasuryCall::reject_proposal(proposal_id) => {
                Ok(("reject_proposal".into(), vec![proposal_id.encode()].encode()))
            },
            TreasuryCall::approve_proposal(proposal_id) => {
                Ok(("approve_proposal".into(), vec![proposal_id.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("".into(), Vec::new()))
            }
        }
    }
}
