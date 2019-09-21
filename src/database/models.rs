//! Database Type Models for Substrate/SubstrateChain/Polkadot Types

use primitive_types::{H256 as SubstrateH256, H512 as SubstrateH512};
use diesel::sql_types::Binary;
use diesel::deserialize::{self, FromSql};
// use super::schema::{blocks, inherants, signed_extrinsics};
use chrono::NaiveDateTime;
// TODO: Make generic
type DB = diesel::pg::Pg;

/// The table for accounts
#[derive(Queryable, PartialEq, Debug)]
pub struct Blocks {
    /// hash of the previous block
    parent_hash: H256,
    /// Hash of this block
    hash: H256,
    /// The block number
    block: usize,
    /// root of the state trie
    state_root: H256,
    /// root of the extrinsics trie
    extrinsics_root: H256,
    /// timestamp
    time: Option<NaiveDateTime>
}

/// Inherants (not signed) extrinsics
#[derive(Queryable, PartialEq, Debug)]
pub struct Inherants {
    /// PostgreSQL Generated ID/Primary Key (No meaning within substrate/chains)
    id: usize,
    /// Hash of the block this inherant was created in, foreign key
    hash: H256,
    /// Block number of the block this inherant was created in
    block: usize,
    /// Module the inherant called
    module: String,
    /// Call within the module inherant used
    call: String,
    /// Was the call succesful?
    success: bool,
    /// Index of the inherant within a block
    in_index: usize,
}

/// Signed Extrinsics (More like traditional transactions)
#[derive(Queryable, PartialEq, Debug)]
pub struct SignedExtrinsics {
    /// Hash of the transaction, primary key
    transaction_hash: H256,
    /// the block this transaction was created in
    block: usize,
    /// the account that originated this transaction
    from_addr: H256,
    /// the account that is receiving this transaction, if any
    to_addr: Option<H256>,
    /// The call this transaction is using
    call: String,
    /// was the transaction succesful?
    success: bool,
    /// nonce of the transaction
    nonce: usize,
    /// Index of the transaction within the block it originated in
    tx_index: usize,
    /// signature of the transaction
    signature: H512
}

/// Accounts  on thechain
#[derive(Queryable, PartialEq, Debug)]
pub struct Accounts {
    /// Address of the account (So far only ed/sr) Primary key
    address: H256,
    /// Free balance of the account
    free_balance: usize,
    /// Reserved balanced
    reserved_balance: usize,
    /// Index of the account within the block it originated in
    account_index: Vec<u8>,
    /// nonce of the account
    nonce: usize,
    /// the block that this account was created in, Foreign key
    create_hash: H256,
    /// Block number that this account was created in
    created: usize,
    /// Block that this account was last updated
    updated: usize,
    /// whether this account is active
    active: bool
}


/// NewType for custom Queryable trait on Substrates H256 type
#[derive(PartialEq, Debug)]
pub struct H256(SubstrateH256);

/// NewType for custom Queryable trait on Substrates H512 type
#[derive(PartialEq, Debug)]
pub struct H512(SubstrateH512);

impl H512 {
    /// Get the H512 back into substrate type
    fn into_inner(self) -> SubstrateH512 {
        self.0
    }
}

impl From<H512> for SubstrateH512 {
    fn from(hash: H512) -> SubstrateH512 {
        hash.into_inner()
    }
}

impl From<SubstrateH512> for H512 {
    fn from(hash: SubstrateH512) -> H512 {
        H512(hash)
    }
}

impl<DB> FromSql<Binary, DB> for H512
where
    DB: diesel::backend::Backend,
    *const [u8]: FromSql<Binary, DB>
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        let vec: &Vec<u8> = &Vec::from_sql(bytes)?;
        Ok(H512(SubstrateH512::from_slice(vec.as_slice())))
    }
}

impl H256 {
    /// Get the H256 back into substrate type
    fn into_inner(self) -> SubstrateH256 {
        self.0
    }
}

impl From<H256> for SubstrateH256 {
    fn from(hash: H256) -> SubstrateH256 {
        hash.into_inner()
    }
}

impl From<SubstrateH256> for H256 {
    fn from(hash: SubstrateH256) -> H256 {
        H256(hash)
    }
}

impl<DB> FromSql<Binary, DB> for H256
where
    DB: diesel::backend::Backend,
    *const [u8]: FromSql<Binary, DB>
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        let vec: &Vec<u8> = &Vec::from_sql(bytes)?;
        Ok(H256(SubstrateH256::from_slice(vec.as_slice())))
    }
}

// Can Either :
// Make Generic over System::Type
//    Therefore, make the external program implement Queryable on types (IE: polkadot-archive)
// Use Diesel types instead of Parity Types -- convert to other types somewhere else along the way
// Use concrete primitives -- requires assumptions -- OK for mvp?
