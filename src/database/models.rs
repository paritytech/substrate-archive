use primitive_types::H256 as SubstrateH256;
use diesel::sql_types::Binary;
use diesel::deserialize::{self, FromSql};
// use super::schema::{blocks, inherants, signed_extrinsics};
use chrono::NaiveDateTime;
// TODO: Make generic
type DB = diesel::pg::Pg;

#[derive(PartialEq, Debug)]
pub struct H256(SubstrateH256);

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

#[derive(PartialEq, Debug)]
pub struct Blocks {
    parent_hash: H256,
    hash: H256,
    block: u64,
    state_root: H256,
    extrinsics_root: H256,
    time: Option<NaiveDateTime>
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
