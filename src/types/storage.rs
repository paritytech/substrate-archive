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

//! Experimental way to manage StorageKey's and their derivative byte-representations
//!

// this may possibly be a nightmare to maintain?
// especially if the # of srml modules continue to change/grow
// keep enums stateless

// TODO: custom derive (macro) implentations for iteration over keys and converting into/from the byte-vec twox_128 hash keys
// TODO: remove dead code attributes, or just get rid of this in favor of a better way/might not need it
// currenty experimental with Timestamp and Finality modules

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StorageKeyType {
    Assets(AssetsOp),
    Aura(AuraOp),
    AuthorityDiscovery(AuthorityDiscoveryOp),
    Authorship(AuthorshipOp),
    Balances(BalancesOp),
    Collective(CollectiveOp),
    Contracts(ContractsOp),
    // Council keeps no storage
    Democracy(DemocracyOp),
    Elections(ElectionsOp),
    ElectionsPhragmen(ElectionsPhragmenOp),
    // executive keeps no storage
    FinalityTracker(FinalityTrackerOp),
    GenericAsset(GenericAssetOp),
    Grandpa(GrandpaOp),
    ImOnline(ImOnlineOp),
    Indices(IndicesOp),
    Membership(MembershipOp),
    // metadata keeps no storage
    Offences(OffencesOp),
    ScoredPool(ScoredPoolOp),
    Session(SessionOp),
    Staking(StakingOp),
    Sudo(SudoOp),
    System(SystemOp),
    Timestamp(TimestampOp),
    Treasury(TreasuryOp),
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AssetsOp {
    Balances,
    NextAssetId,
    TotalSupply,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AuraOp {
    LastTimestamp,
    Authorities,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AuthorityDiscoveryOp {
    Keys,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AuthorshipOp {
    Uncles,
    Author,
    DidSetUncles,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BalancesOp {
    TotalIssuance,
    Vesting,
    FreeBalance,
    ReservedBalance,
    Locks,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CollectiveOp {
    Proposals,
    ProposalOf,
    Voting,
    ProposalCount,
    Members,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ContractsOp {
    GasSpent,
    CurrentSchedule,
    PristineCode,
    CodeStorage,
    AccountCounter,
    ContractInfoOf,
    GasPrice,
}

// ---- Council has no storage -----

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DemocracyOp {
    PublicPropCount,
    PublicProps,
    DepositOf,
    ReferendumCount,
    NextTally,
    ReferendumInfoOf,
    DispatchQueue,
    VotersFor,
    VoteOf,
    Proxy,
    Delegations,
    LastTabledWasExternal,
    NextExternal,
    Blacklist,
    Cancellations,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ElectionsOp {
    // parameters
    PresentationDuration,
    TermDuration,
    DesiredSeats,
    // permanent state
    Members,
    VoteCount,
    // persistent state
    ApprovalsOf,
    RegisterInfoOf,
    VoterInfoOf,
    Voters,
    NextVoterSet,
    VoterCount,
    Candidates,
    CandidateCount,
    // temporary state (only relevent during finalization/presentation)
    NextFinalize,
    Leaderboard,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ElectionsPhragmenOp {
    // params
    DesiredMembers,
    DesiredRunnersUp,
    TermDuration,
    // State
    Members,
    RunnersUp,
    ElectionRounds,
    VotesOf,
    StakeOf,
    Candidates,
}

// -- Executive keeps no storage --

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FinalityTrackerOp {
    RecentHints,
    OrderedHints,
    Median,
    Update,
    Initialized,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum GenericAssetOp {
    TotalIssuance,
    FreeBalance,
    ReservedBalance,
    NextAssetId,
    Permissions,
    Locks,
    StakingAssetId,
    SpendingAssetId,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum GrandpaOp {
    Authorities,
    State,
    PendingChange,
    NextForced,
    Stalled,
    CurrentSetId,
    SetIdSession,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ImOnlineOp {
    GossipAt,
    Keys,
    ReceivedHeartbeats,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum IndicesOp {
    NextEnumSet,
    EnumSet,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MembershipOp {
    Members,
}

// --- Metadata keeps no storage --

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum OffencesOp {
    Reports,
    ConcurrentReportsIndex,
    ReportsByKindIndex,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ScoredPoolOp {
    Pool,
    CandidateExists,
    Members,
    MemberCount,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SessionOp {
    Validators,
    CurrentIndex,
    QueuedChanged,
    QueuedKeys,
    DisabledValidators,
    NextKeys,
    KeyOwner,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum StakingOp {
    ValidatorCount,
    MinimumValidatorCount,
    Invulnerables,
    Bonded,
    Ledger,
    Payee,
    Validators,
    Nominators,
    Stakers,
    CurrentElected,
    CurrentEra,
    CurrentEraStart,
    CurrentEraStartSessionIndex,
    CurrentEraPointsEarned,
    SlotStake,
    ForceEra,
    SlashRewardFraction,
    BondedEras,
    EraSlashJournal,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SudoOp {
    Key,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SystemOp {
    AccountNonce,
    /// Total extrinsics count for the current block
    ExtrinsicCount,
    /// Total weight for all extrinsics put together, for the current block.
    AllExtrinsicsWeight,
    /// Total length (in bytes) for all extrinsics put together, for the current block.
    AllExtrinsicsLen,
    /// The next weight multiplier. This should be updated at the end of each block based on the
    /// saturation level (weight).
    NextWeightMultiplier,
    /// Map of block numbers to block hashes.
    BlockHash,
    /// Extrinsics data for the current block (maps an extrinsic's index to its data).
    ExtrinsicData,
    /// Series of block headers from the last 81 blocks that acts as random seed material. This is arranged as a
    /// ring buffer with the `i8` prefix being the index into the `Vec` of the oldest hash.
    RandomMaterial,
    /// The current block number being processed. Set by `execute_block`.
    Number,
    /// Hash of the previous block.
    ParentHash,
    /// Extrinsics root of the current block, also part of the block header.
    ExtrinsicsRoot,
    /// Digest of the current block, also part of the block header.
    Digest,
    /// Events deposited for the current block.
    Events,
    /// The number of events in the `Events<T>` list.
    EventCount,

    /// Mapping between a topic (represented by T::Hash) and a vector of indexes
    /// of events in the `<Events<T>>` list.
    ///
    /// The first key serves no purpose. This field is declared as double_map just
    /// for convenience of using `remove_prefix`.
    ///
    /// All topic vectors have deterministic storage locations depending on the topic. This
    /// allows light-clients to leverage the changes trie storage tracking mechanism and
    /// in case of changes fetch the list of events of interest.
    ///
    /// The value has the type `(T::BlockNumber, EventIndex)` because if we used only just
    /// the `EventIndex` then in case if the topic has the same contents on the next block
    /// no notification will be triggered thus the event might be lost.
    EventTopics,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TimestampOp {
    Now,
    DidUpdate,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TreasuryOp {
    ProposalCount,
    Proposals,
    Approvals,
}
