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

// TODO: THE NEW WAY:
// Get name of Module + Name of Call
// Don't do anything else
// Call into Storage()
// Get EVERYTHING WE NEED :)

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
use srml_nicks::Call as NicksCall;
use srml_elections_phragmen::Call as ElectionsPhragmenCall;
use srml_balances::Call as BalancesCall;
use srml_system::Call as SystemCall;
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
                Ok(("__phantom".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for BabeCall<T> where T: srml_babe::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            &__phantom_item => {
                Ok(("__phantom".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for BalancesCall<T> where T: srml_balances::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            BalancesCall::transfer(dest, value) => {
                Ok(("transfer".into(), vec![dest.encode(), value.encode()].encode()))
            },
            BalancesCall::set_balance(who, new_free, new_reserved) => {
                Ok(("set_balance".into(), vec![who.encode(), new_free.encode(), new_reserved.encode()].encode()))
            },
            BalancesCall::force_transfer(source, dest, value) => {
                Ok(("force_transfer".into(), vec![source.encode(), dest.encode(), value.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("__phantom".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for ElectionsPhragmenCall<T> where T: srml_elections_phragmen::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            ElectionsPhragmenCall::vote(votes, value) => {
                Ok(("vote".into(), vec![votes.encode(), value.encode()].encode()))
            },
            ElectionsPhragmenCall::remove_voter() => {
                Ok(("remove_voter".into(), Vec::new()))
            },
            ElectionsPhragmenCall::report_defunct_voter(target) => {
                Ok(("report_defunct_voter".into(), vec![target.encode()].encode()))
            },
            ElectionsPhragmenCall::submit_candidacy() => {
                Ok(("submit_candidacy".into(), Vec::new()))
            },
            /*ElectionsPhragmenCall::set_desired_member_count(count) => {
                Ok(("set_desired_member_count".into(), vec![count.encode()].encode()))
            },*/
            ElectionsPhragmenCall::remove_member(who) => {
                Ok(("remove_member".into(), vec![who.encode()].encode()))
            },
            /*ElectionsPhragmenCall::set_term_duration(count) => {
                Ok(("set_term_duration".into(), vec![count.encode()].encode()))
            },*/
            &__phantom_item => {
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
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
                 Ok(("__phantom".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for NicksCall<T> where T: srml_nicks::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            NicksCall::set_name(name) => {
                Ok(("set_name".into(), vec![name.encode()].encode()))
            },
            NicksCall::clear_name() => {
                Ok(("clear_name".into(), Vec::new()))
            },
            NicksCall::kill_name(target) => {
                Ok(("kill_name".into(), vec![target.encode()].encode()))
            },
            NicksCall::force_name(target, name) => {
                Ok(("force_name".into(), vec![target.encode(), name.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
            }
        }
    }
}

impl<T> SrmlExt for SystemCall<T> where T: srml_system::Trait {
    fn function(&self) -> SrmlResult<FunctionInfo> {
        match &self {
            SystemCall::fill_block() => {
                Ok(("fill_block".into(), Vec::new()))
            },
            SystemCall::remark(remark) => {
                Ok(("remark".into(), vec![remark.encode()].encode()))
            },
            SystemCall::set_heap_pages(pages) => {
                Ok(("set_heap_pages".into(), vec![pages.encode()].encode()))
            },
            SystemCall::set_code(new) => {
                Ok(("set_code".into(), vec![new.encode()].encode()))
            },
            SystemCall::set_storage(items) => {
                Ok(("set_storage".into(), vec![items.encode()].encode()))
            },
            SystemCall::kill_storage(keys) => {
                Ok(("kill_storage".into(), vec![keys.encode()].encode()))
            },
            SystemCall::kill_prefix(prefix) => {
                Ok(("kill_prefix".into(), vec![prefix.encode()].encode()))
            },
            &__phantom_item => {
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
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
                Ok(("__phantom".into(), Vec::new()))
            }
        }
    }
}
