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

//! Extensions to Frame Modules to extract data useful in a database scenario

// TODO: THE NEW WAY:
// Get name of Module + Name of Call
// Don't do anything else
// Call into Storage()
// Get EVERYTHING WE NEED :)
mod types;
use self::types::*;
use frame_system::Call as SystemCall;
use log::trace;
use pallet_aura::Call as AuraCall;
use pallet_babe::Call as BabeCall;
use pallet_balances::Call as BalancesCall;
use pallet_democracy::Call as DemocracyCall;
use pallet_elections_phragmen::Call as ElectionsPhragmenCall;
use pallet_finality_tracker::Call as FinalityCall;
use pallet_grandpa::Call as GrandpaCall;
use pallet_im_online::Call as ImOnlineCall;
use pallet_nicks::Call as NicksCall;
use pallet_session::Call as SessionCall;
use pallet_staking::Call as StakingCall;
use pallet_sudo::Call as SudoCall;
use pallet_timestamp::Call as TimestampCall;
use pallet_treasury::Call as TreasuryCall;
use serde_json::{json, Value};
// use runtime_support::dispatch::{IsSubType, Callable};
use codec::Encode;

use crate::error::Error;

pub trait FrameExt: std::fmt::Debug {
    /// Seperates a call into it's name and parameters
    /// Parameters are SCALE encoded
    fn function(&self) -> Result<(CallName, Parameters), Error>; // name of the function as a string
}

/// Name of the function
pub type CallName = String;
/// SCALE Encoded Parameters
pub type Parameters = Value;

// TODO: look to store parameters in something other than SCALE
// like raw bit-array
// problem is for modules that contain data other than simple u32's (ex: sudo)
/// Convenience type
pub type FunctionInfo = (CallName, Parameters);
type FrameResult<T> = Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotHandled;

impl FrameExt for NotHandled {
    fn function(&self) -> FrameResult<FunctionInfo> {
        Err(Error::UnhandledCallType)
    }
}

/*
impl<T> FrameExt for AssetsCall<T> where T: pallet_assets::Trait {
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            AssetsCall::balances(b) => {
                Ok(("balances".into(), b.encode()))
            }
        }
    }
}
 */

impl<T> FrameExt for AuraCall<T>
where
    T: pallet_aura::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for BabeCall<T>
where
    T: pallet_babe::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for BalancesCall<T>
where
    T: pallet_balances::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            BalancesCall::transfer(dest, value) => {
                let val = json!([
                    { "dest": dest.encode(), "encoded": true },
                    { "value": value },
                ]);
                Ok(("transfer".into(), val))
            }
            BalancesCall::set_balance(who, new_free, new_reserved) => {
                let val = json!([
                    { "who": who.encode(), "encoded": true },
                    { "new_free": new_free },
                    { "new_reserved": new_reserved }
                ]);
                Ok(("set_balance".into(), val))
            }
            BalancesCall::force_transfer(source, dest, value) => {
                let val = json!([
                    { "source": source.encode(), "encoded": true },
                    { "dest": dest.encode(), "encoded": true },
                    { "value": value }
                ]);
                Ok(("force_transfer".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for DemocracyCall<T>
where
    T: pallet_democracy::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            DemocracyCall::propose(proposal_hash, value) => {
                let val = json!([{ "proposal_hash": proposal_hash }, { "value": value }]);
                Ok(("propose".into(), val))
            }
            DemocracyCall::second(proposal) => {
                let val = json!([{ "proposal": proposal }]);
                Ok(("second".into(), val))
            }
            DemocracyCall::vote(ref_index, vote) => {
                let vote =
                    serde_json::to_string(&VoteHelper(*vote)).expect("Vote should not fail; qed");

                let val = json!([{ "ref_index": ref_index }, { "vote": vote }]);

                Ok(("vote".into(), val))
            }
            DemocracyCall::proxy_vote(ref_index, vote) => {
                let vote =
                    serde_json::to_string(&VoteHelper(*vote)).expect("Vote should not fail; qed");
                let val = json!([{ "ref_index": ref_index, "vote": vote }]);

                Ok(("proxy_vote".into(), val))
            }
            DemocracyCall::emergency_cancel(ref_index) => {
                let val = json!([{ "ref_index": ref_index }]);

                Ok(("emergency_cancel".into(), val))
            }
            DemocracyCall::external_propose(proposal_hash) => {
                let val = json!([{ "proposal_hash": proposal_hash }]);

                Ok(("external_propose".into(), val))
            }
            DemocracyCall::external_propose_majority(proposal_hash) => {
                let val = json!([{ "proposal_hash": proposal_hash }]);

                Ok(("external_propose_majority".into(), val))
            }
            DemocracyCall::external_propose_default(proposal_hash) => {
                let val = json!([{ "proposal_hash": proposal_hash }]);

                Ok(("external_propose_default".into(), val))
            }
            DemocracyCall::fast_track(proposal_hash, voting_period, delay) => {
                let val = json!([
                    { "proposal_hash": proposal_hash },
                    { "voting_period": voting_period },
                    { "delay": delay }
                ]);

                Ok(("fast_track".into(), val))
            }
            DemocracyCall::veto_external(proposal_hash) => {
                let val = json!([{ "proposal_hash": proposal_hash }]);

                Ok(("veto_external".into(), val))
            }
            DemocracyCall::cancel_referendum(ref_index) => {
                let val = json!([{ "ref_index": ref_index }]);

                Ok(("cancel_referendum".into(), val))
            }
            DemocracyCall::cancel_queued(when, which, what) => {
                let val = json!([{ "when": when }, { "which": which }, { "what": what }]);

                Ok(("cancel_queued".into(), val))
            }
            DemocracyCall::set_proxy(proxy) => {
                let val = json!([{ "proxy": proxy }]);

                Ok(("set_proxy".into(), val))
            }
            DemocracyCall::resign_proxy() => Ok(("resign_proxy".into(), json!([]))),
            DemocracyCall::remove_proxy(proxy) => {
                let val = json!([{ "proxy": proxy }]);

                Ok(("remove_proxy".into(), val))
            }
            DemocracyCall::delegate(to, conviction) => {
                let conviction = serde_json::to_string(&ConvictionHelper(*conviction))
                    .expect("Serialize infallible; qed");
                let val = json!([{ "to": to }, { "conviction": conviction }]);

                Ok(("delegate".into(), val))
            }
            DemocracyCall::undelegate() => Ok(("undelegate".into(), json!([]))),
            DemocracyCall::clear_public_proposals() => {
                Ok(("clear_public_proposals".into(), json!([])))
            }
            DemocracyCall::note_preimage(encoded_proposal) => {
                let val = json!([{ "encoded_proposal": encoded_proposal, "encoded": true }]);

                Ok(("note_preimage".into(), val))
            }
            DemocracyCall::note_imminent_preimage(encoded_proposal, when, which) => {
                let val = json!([
                    {"encoded_proposal": encoded_proposal, "encoded": true},
                    {"when": when},
                    {"which": which}
                ]);
                Ok(("note_imminent_preimage".into(), val))
            }
            DemocracyCall::reap_preimage(proposal_hash) => {
                let val = json!([{ "proposal_hash": proposal_hash },]);

                Ok(("reap_preimage".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for ElectionsPhragmenCall<T>
where
    T: pallet_elections_phragmen::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            ElectionsPhragmenCall::vote(votes, value) => {
                let val = json!([{ "votes": votes }, { "value": value }]);
                Ok(("vote".into(), val))
            }
            ElectionsPhragmenCall::remove_voter() => Ok(("remove_voter".into(), json!({}))),
            ElectionsPhragmenCall::report_defunct_voter(target) => {
                let val = json!([
                    { "target": target.encode(), "encoded": true }
                ]);
                Ok(("report_defunct_voter".into(), val))
            }
            ElectionsPhragmenCall::submit_candidacy() => Ok(("submit_candidacy".into(), json!({}))),
            /*ElectionsPhragmenCall::set_desired_member_count(count) => {
                Ok(("set_desired_member_count".into(), vec![count.encode()].encode()))
            },*/
            ElectionsPhragmenCall::remove_member(who) => {
                let val = json!([
                    {"who": who.encode(), "encoded": true }
                ]);
                Ok(("remove_member".into(), val))
            }
            /*ElectionsPhragmenCall::set_term_duration(count) => {
                Ok(("set_term_duration".into(), vec![count.encode()].encode()))
            },*/
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for SessionCall<T>
where
    T: pallet_session::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            SessionCall::set_keys(keys, proof) => {
                let val = json!([
                    { "keys": keys.encode(), "encoded": true },
                    { "proof": proof }
                ]);
                Ok(("set_keys".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

// matching exhaustively on &__phantom_item allows the compiler to implicitly
// check making sure all Call types are covered
impl<T> FrameExt for TimestampCall<T>
where
    T: pallet_timestamp::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            TimestampCall::set(time) => {
                let val = json!([
                    { "time": time.encode(), "encoded": true }
                ]);
                Ok(("set".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for FinalityCall<T>
where
    T: pallet_finality_tracker::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            FinalityCall::final_hint(block) => {
                let val = json!([{ "block": block }]);
                Ok(("final_hint".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for ImOnlineCall<T>
where
    T: pallet_im_online::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            ImOnlineCall::heartbeat(heartbeat, signature) => {
                let val = json!([
                    { "heartbeat": heartbeat.encode(), "encoded": true },
                    { "signature": signature.encode(), "encoded": true }
                ]);
                Ok(("im-online".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for NicksCall<T>
where
    T: pallet_nicks::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            NicksCall::set_name(name) => {
                let val = json!([{ "name": name }]);
                Ok(("set_name".into(), val))
            }
            NicksCall::clear_name() => Ok(("clear_name".into(), json!({}))),
            NicksCall::kill_name(target) => {
                let val = json!([
                    { "target": target.encode(), "encoded": true }
                ]);
                Ok(("kill_name".into(), val))
            }
            NicksCall::force_name(target, name) => {
                let val = json!([
                    { "name": name },
                    { "target": target.encode() }
                ]);
                Ok(("force_name".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for StakingCall<T>
where
    T: pallet_staking::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            StakingCall::bond(controller, value, payee) => {
                let payee = serde_json::to_string(&RewardDestinationHelper(*payee))
                    .expect("serialization is infallible; qed");
                let val = json!([
                    { "controller": controller.encode(), "encoded": true },
                    { "value": value },
                    { "payee": payee }
                ]);
                Ok(("bond".into(), val))
            }
            StakingCall::bond_extra(max_additional) => {
                let val = json!([{ "max_additional": max_additional }]);
                Ok(("bond_extra".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for SystemCall<T>
where
    T: frame_system::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            SystemCall::fill_block() => Ok(("fill_block".into(), json!({}))),
            SystemCall::remark(remark) => {
                let val = json!([{ "remark": remark }]);
                Ok(("remark".into(), val))
            }
            SystemCall::set_heap_pages(pages) => {
                let val = json!([{ "pages": pages }]);
                Ok(("set_heap_pages".into(), val))
            }
            SystemCall::set_code(new) => {
                let val = json!([{ "new": new }]);
                Ok(("set_code".into(), val))
            }
            SystemCall::set_storage(items) => {
                let val = json!([{ "items": items }]);
                Ok(("set_storage".into(), val))
            }
            SystemCall::kill_storage(keys) => {
                let val = json!([{ "keys": keys }]);
                Ok(("kill_storage".into(), val))
            }
            SystemCall::kill_prefix(prefix) => {
                let val = json!([{ "prefix": prefix }]);
                Ok(("kill_prefix".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for GrandpaCall<T>
where
    T: pallet_grandpa::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            GrandpaCall::report_misbehavior(report) => {
                let val = json!([{ "report": report }]);
                Ok(("report_misbehavior".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for SudoCall<T>
where
    T: pallet_sudo::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            SudoCall::sudo(proposal) => {
                let val = json!([
                    { "proposal": proposal.encode(), "encoded": true }
                ]);
                // let public_call: T::Proposal =
                trace!("PROPOSAL: {:?}", proposal);
                Ok(("sudo".into(), val))
            }
            SudoCall::set_key(new) => {
                let val = json!([
                    { "new": new.encode(), "encoded": true }
                ]);
                Ok(("set_key".into(), val))
            }
            SudoCall::sudo_as(who, proposal) => {
                let val = json!([
                    {"who": who.encode(), "encoded": true},
                    {"proposal": proposal.encode(), "encoded": true}
                ]);
                Ok(("sudo_as".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}

impl<T> FrameExt for TreasuryCall<T>
where
    T: pallet_treasury::Trait,
{
    fn function(&self) -> FrameResult<FunctionInfo> {
        match &self {
            TreasuryCall::propose_spend(value, beneficiary) => {
                let val = json!([
                    { "value": value },
                    { "beneficiary": beneficiary.encode(), "encoded": true }
                ]);
                Ok(("propose_spend".into(), val))
            }
            TreasuryCall::reject_proposal(proposal_id) => {
                let val = json!([{ "proposal_id": proposal_id }]);
                Ok(("reject_proposal".into(), val))
            }
            TreasuryCall::approve_proposal(proposal_id) => {
                let val = json!([{ "proposal_id": proposal_id }]);
                Ok(("approve_proposal".into(), val))
            }
            &__phantom_item => Ok(("__phantom".into(), json!({}))),
        }
    }
}
