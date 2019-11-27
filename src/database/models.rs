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

//! Database Type Models for Substrate/SubstrateChain/Polkadot Types

use primitive_types::{H256 as SubstrateH256, H512 as SubstrateH512};
// use codec::Decode;
use chrono::{offset::Utc, DateTime};
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::sql_types::Binary;
use diesel::{AsChangeset, Queryable};

use super::schema::{accounts, blocks, inherents, signed_extrinsics};

// TODO: Make generic

/// Inserting a block
/// everything serialized as raw bytes
/// if not originally a hash (bytes from hashes are extracted with as_ref())
/// it is encoded with parity_scale_codec (u32/etc)
/// to make up for PostgreSQL's lack of an unsigned data type
#[derive(Insertable, AsChangeset)]
#[table_name = "blocks"]
pub struct InsertBlock<'a> {
    pub parent_hash: &'a [u8],
    pub hash: &'a [u8],
    pub block_num: &'a i64,
    pub state_root: &'a [u8],
    pub extrinsics_root: &'a [u8],
    pub time: Option<&'a DateTime<Utc>>,
}

#[derive(Insertable, AsChangeset)]
#[table_name = "blocks"]
pub struct InsertBlockOwned {
    pub parent_hash: Vec<u8>,
    pub hash: Vec<u8>,
    pub block_num: i64,
    pub state_root: Vec<u8>,
    pub extrinsics_root: Vec<u8>,
    pub time: Option<DateTime<Utc>>,
}

#[derive(Insertable)]
#[table_name = "inherents"]
pub struct InsertInherent<'a> {
    pub hash: &'a [u8],
    pub block_num: &'a i64,
    pub module: &'a str,
    pub call: &'a str,
    pub parameters: Option<Vec<u8>>,
    // pub success: &'a bool,
    pub in_index: &'a i32,
    pub transaction_version: &'a i32,
}

// for batch inserts where collecting references may not always live long enough
#[derive(Insertable, Debug)]
#[table_name = "inherents"]
pub struct InsertInherentOwned {
    pub hash: Vec<u8>,
    pub block_num: i64,
    pub module: String,
    pub call: String,
    pub parameters: Option<Vec<u8>>,
    // pub success: bool,
    pub in_index: i32,
    pub transaction_version: i32,
}

#[derive(Insertable)]
#[table_name = "signed_extrinsics"]
pub struct InsertTransaction<'a> {
    pub transaction_hash: &'a [u8],
    pub block_num: &'a i64,
    pub hash: &'a [u8],
    pub from_addr: &'a [u8],
    pub to_addr: Option<&'a [u8]>,
    pub call: &'a str,
    // pub success: &'a bool,
    pub nonce: &'a i32,
    pub tx_index: &'a i32,
    pub signature: &'a [u8],
    pub transaction_version: &'a i32,
}

#[derive(Insertable, Debug)]
#[table_name = "signed_extrinsics"]
pub struct InsertTransactionOwned {
    pub transaction_hash: Vec<u8>,
    pub block_num: i64,
    pub hash: Vec<u8>,
    pub from_addr: Vec<u8>,
    pub to_addr: Option<Vec<u8>>,
    pub call: String,
    // pub success: bool,
    pub nonce: i32,
    pub tx_index: i32,
    pub signature: Vec<u8>,
    pub transaction_version: i32,
}

#[derive(Insertable)]
#[table_name = "accounts"]
pub struct InsertAccount<'a> {
    address: &'a [u8],
    free_balance: &'a i64,
    reserved_balance: &'a i64,
    account_index: &'a [u8],
    nonce: &'a i32,
    create_hash: &'a [u8],
    created: &'a i64,
    updated: &'a i64,
    active: &'a bool,
}

type EncodedData = Vec<u8>;

/// The table for accounts
#[derive(Queryable, PartialEq, Debug)]
pub struct Blocks {
    /// hash of the previous block
    pub parent_hash: H256,
    /// Hash of this block
    pub block_hash: H256,
    /// The block number
    pub block_num: i64,
    /// root of the state trie
    pub state_root: H256,
    /// root of the extrinsics trie
    pub extrinsics_root: H256,
    /// timestamp
    pub time: Option<DateTime<Utc>>,
}

/// Inherents (not signed) extrinsics
#[derive(Queryable, PartialEq, Debug)]
pub struct Inherents {
    /// PostgreSQL Generated ID/Primary Key (No meaning within substrate/chains)
    id: i32,
    /// Hash of the block this inherant was created in, foreign key
    hash: H256,
    /// Block number of the block this inherant was created in
    block: i64,
    /// Module the inherant called
    module: String,
    /// Call within the module inherant used
    call: String,
    parameters: Option<Vec<u8>>,
    /// Was the call succesful?
    // success: bool,
    /// Index of the inherant within a block
    in_index: i32,
}

/// Signed Extrinsics (More like traditional transactions)
#[derive(Queryable, PartialEq, Debug)]
pub struct SignedExtrinsics {
    /// Hash of the transaction, primary key
    transaction_hash: H256,
    /// the block this transaction was created in
    block_num: i64,
    /// the account that originated this transaction
    from_addr: H256,
    /// the account that is receiving this transaction, if any
    to_addr: Option<H256>,
    /// The call this transaction is using
    call: String,
    /// was the transaction succesful?
    // success: bool,
    /// nonce of the transaction
    nonce: usize,
    /// Index of the transaction within the block it originated in
    tx_index: usize,
    /// signature of the transaction
    signature: H512,
}

/// Accounts  on thechain
#[derive(Queryable, PartialEq, Debug)]
pub struct Accounts {
    // TODO: Use b58 addr format or assign trait..overall make generic
    /// Address of the account (So far only ed/sr) Primary key
    address: EncodedData,
    /// Free balance of the account
    free_balance: usize,
    /// Reserved balanced
    reserved_balance: usize,
    /// Index of the account within the block it originated in
    account_index: i64,
    /// nonce of the account
    nonce: usize,
    /// the block that this account was created in, Foreign key
    create_hash: H256,
    /// Block number that this account was created in
    created: i64,
    /// Block that this account was last updated
    updated: i64,
    /// whether this account is active
    active: bool,
}

/// NewType for custom Queryable trait on Substrates H256 type
#[derive(FromSqlRow, PartialEq, Debug)]
pub struct H256(SubstrateH256);

/*
impl Queryable<Binary, DB> for H256 {
    type Row = Binary;

    fn build(row: Self::Row) -> Self {
        let vec: Vec<u8> = row::from_sql();
        H256(Substrate::H256::from_slice(vec.as_slice()))
    }
}
*/

/// NewType for custom Queryable trait on Substrates H512 type
#[derive(FromSqlRow, PartialEq, Debug)]
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
    DB: Backend,
    *const [u8]: FromSql<Binary, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        let vec: &Vec<u8> = &Vec::from_sql(bytes)?;
        Ok(H512(SubstrateH512::from_slice(vec.as_slice())))
    }
}
/*
impl<DB> FromSql<Binary, DB> for EncodedUint
where
    DB: Backend,
    *const [u8]: FromSql<Binary, DB>
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        Vec::from_sql(bytes)?
    }
}
*/

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
    DB: Backend,
    *const [u8]: FromSql<Binary, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        let vec: &Vec<u8> = &Vec::from_sql(bytes)?;
        Ok(H256(SubstrateH256::from_slice(vec.as_slice())))
    }
}

// Can Either :
// Make Generic over System::Type
//    Therefore, make the external program implement Queryable on types (IE: polkadot-archive)
//
// Use Diesel types instead of Parity Types -- convert to other types somewhere else along the way
//
// Use concrete primitives -- requires assumptions -- OK for mvp?
//
// OR just don't use any types at all and encode everything as a Vec<u8>.
// This loses some meaning for the type, but it is the easiest and fastest way to implement a form of
// generalization of chains
// it just leaves the type conversions up to the end user
// which isn't the most ergonomic thing
