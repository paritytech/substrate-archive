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

use pallet_democracy::{Conviction, Vote};
use pallet_staking::RewardDestination;
use serde::Serialize;

////////////////////////
// Democracy Types //
////////////////////////
#[derive(Serialize)]
#[serde(remote = "Conviction")]
pub enum ConvictionDef {
    /// 0.1x votes, unlocked.
    None,
    /// 1x votes, locked for an enactment period following a successful vote.
    Locked1x,
    /// 2x votes, locked for 2x enactment periods following a successful vote.
    Locked2x,
    /// 3x votes, locked for 4x...
    Locked3x,
    /// 4x votes, locked for 8x...
    Locked4x,
    /// 5x votes, locked for 16x...
    Locked5x,
    /// 6x votes, locked for 32x...
    Locked6x,
}

#[derive(Serialize)]
#[serde(remote = "Vote")]
pub struct VoteDef {
    pub aye: bool,
    #[serde(with = "ConvictionDef")]
    pub conviction: Conviction,
}

#[derive(Serialize)]
pub struct VoteHelper(#[serde(with = "VoteDef")] pub Vote);

#[derive(Serialize)]
pub struct ConvictionHelper(#[serde(with = "ConvictionDef")] pub Conviction);

//////////////////////
// Staking Types //
//////////////////////
#[derive(Serialize)]
#[serde(remote = "RewardDestination")]
pub enum RewardDestinationDef {
    Staked,
    Stash,
    Controller,
}

#[derive(Serialize)]
pub struct RewardDestinationHelper(#[serde(with = "RewardDestinationDef")] pub RewardDestination);
