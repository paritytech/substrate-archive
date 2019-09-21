//! Database Type Models for Substrate/SubstrateChain/Polkadot Types

use primitive_types::{H256 as SubstrateH256, H512 as SubstrateH512};
use diesel::sql_types::Binary;
use diesel::deserialize::{self, FromSql};
// use super::schema::{blocks, inherants, signed_extrinsics};
use chrono::NaiveDateTime;
// TODO: Make generic
type DB = diesel::pg::Pg;


#[derive(Queryable, PartialEq, Debug)]
pub struct Blocks {
    parent_hash: H256,
    hash: H256,
    block: usize,
    state_root: H256,
    extrinsics_root: H256,
    time: Option<NaiveDateTime>
}

#[derive(Queryable, PartialEq, Debug)]
pub struct Inherants {
    id: usize,
    hash: H256,
    block: usize,
    module: String,
    call: String,
    success: bool,
    in_index: usize,
}

#[derive(Queryable, PartialEq, Debug)]
pub struct SignedExtrinsics {
    transaction_hash: H256,
    block: usize,
    from_addr: H256,
    to_addr: Option<H256>,
    call: String,
    success: bool,
    nonce: usize,
    tx_index: usize,
    signature: H512
}

#[derive(Queryable, PartialEq, Debug)]
pub struct Accounts {
    address: H256,
    free_balance: usize,
    reserved_balance: usize,
    account_index: Vec<u8>,
    nonce: usize,
    create_hash: H256,
    created: usize,
    updated: usize,
    active: bool
}


#[derive(PartialEq, Debug)]
pub struct H256(SubstrateH256);
#[derive(PartialEq, Debug)]
pub struct H512(SubstrateH512);

impl H512 {
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
