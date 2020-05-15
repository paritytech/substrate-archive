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

//! prepared statements for sqlx

use crate::error::{ArchiveResult, Error as ArchiveError};
use crate::types::*;
use sp_runtime::traits::Header as _;
use sqlx::PgConnection;
use sqlx::Postgres;
use subxt::system::System;

pub trait PrepareSql<'a> {
    /// prepare a query for insertion
    fn prep_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
    /// add values to a query (to do a batch insert, for instance)
    fn add(&self, query: sqlx::Query<'a, Postgres>) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
}

impl<'a, T> PrepareSql<'a> for Block<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: From<u32>,
    <T as System>::BlockNumber: Into<u32>,
{
    fn prep_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let parent_hash = self.inner.block.header.parent_hash().as_ref();
        let hash = self.inner.block.header.hash();
        let block_num: u32 = (*self.inner.block.header.number()).into();
        let state_root = self.inner.block.header.state_root().as_ref();
        let extrinsics_root = self.inner.block.header.extrinsics_root().as_ref();

        Ok(sqlx::query(
            r#"
INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root)
VALUES ($1, $2, $3, $4, $5)
"#,
        )
        .bind(parent_hash)
        .bind(hash.as_ref())
        .bind(block_num)
        .bind(state_root)
        .bind(extrinsics_root))
    }

    fn add(&self, query: sqlx::Query<'a, Postgres>) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let parent_hash = self.inner.block.header.parent_hash().as_ref();
        let hash = self.inner.block.header.hash();
        let block_num: u32 = (*self.inner.block.header.number()).into();
        let state_root = self.inner.block.header.state_root().as_ref();
        let extrinsics_root = self.inner.block.header.extrinsics_root().as_ref();

        Ok(query
            .bind(parent_hash)
            .bind(hash.as_ref())
            .bind(block_num)
            .bind(state_root)
            .bind(extrinsics_root))
    }
}

impl<'a, T> PrepareSql<'a> for Extrinsic<T>
where
    T: Substrate + Send + Sync,
{
    fn prep_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        if self.is_signed() {
            // FIXME
            // workaround for serde not serializing u128 to value
            // and diesel only supporting serde_Json::Value for jsonb in postgres
            // u128 are used in balance transfers
            let parameters = serde_json::to_string(&self.args())?;
            let parameters: serde_json::Value = serde_json::from_str(&parameters)?;
            let (addr, sig, extra) = self
                .signature()
                .ok_or(ArchiveError::DataNotFound("Signature".to_string()))?
                .parts();

            let addr = serde_json::to_string(addr)?;
            let sig = serde_json::to_string(sig)?;
            let extra = serde_json::to_string(extra)?;
            let addr: serde_json::Value = serde_json::from_str(&addr)?;
            let sig: serde_json::Value = serde_json::from_str(&sig)?;
            let extra: serde_json::Value = serde_json::from_str(&extra)?;

            Ok(sqlx::query(r#"
INSERT INTO signed_extrinsics (hash, block_num, from_addr, module, call, parameters, tx_index, signature, extra, transaction_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"#
            )
                .bind(self.hash().as_ref())
                .bind(self.block_num())
                .bind(addr)
                .bind(self.ext_module())
                .bind(self.ext_call())
                .bind(Some(parameters))
                .bind(self.index() as u32)
                .bind(sig)
                .bind(Some(extra))
                .bind(0)) // FIXME: Transaction version incorrect
        } else {
            // FIXME
            // workaround for serde not serializing u128 to value
            // and diesel only supporting serde_Json::Value for jsonb in postgres
            // u128 are used in balance transfers
            let parameters = serde_json::to_string(&self.args()).unwrap();
            let parameters: serde_json::Value = serde_json::from_str(&parameters).unwrap();

            Ok(sqlx::query(
                r#"
INSERT INTO inherents (hash, block_num, module, call, parameters, in_index, transaction_version)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"#,
            )
            .bind(self.hash().as_ref())
            .bind(self.block_num())
            .bind(self.ext_module())
            .bind(self.ext_call())
            .bind(Some(parameters))
            .bind(self.index() as u32)
            .bind(0 as u32))
        }
    }

    fn add(&self, query: sqlx::Query<'a, Postgres>) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        if self.is_signed() {
            // FIXME
            // workaround for serde not serializing u128 to value
            // and diesel only supporting serde_Json::Value for jsonb in postgres
            // u128 are used in balance transfers
            let parameters = serde_json::to_string(&self.args())?;
            let parameters: serde_json::Value = serde_json::from_str(&parameters)?;
            let (addr, sig, extra) = self
                .signature()
                .ok_or(ArchiveError::DataNotFound("Signature".to_string()))?
                .parts();

            let addr = serde_json::to_string(addr)?;
            let sig = serde_json::to_string(sig)?;
            let extra = serde_json::to_string(extra)?;

            let addr: serde_json::Value = serde_json::from_str(&addr)?;
            let sig: serde_json::Value = serde_json::from_str(&sig)?;
            let extra: serde_json::Value = serde_json::from_str(&extra)?;

            Ok(query
                .bind(self.hash().as_ref())
                .bind(self.block_num())
                .bind(addr)
                .bind(self.ext_module())
                .bind(self.ext_call())
                .bind(Some(parameters))
                .bind(self.index() as u32)
                .bind(sig)
                .bind(Some(extra))
                .bind(0)) // FIXME: Transaction version incorrect
        } else {
            // FIXME
            // workaround for serde not serializing u128 to value
            // and diesel only supporting serde_Json::Value for jsonb in postgres
            // u128 are used in balance transfers
            let parameters = serde_json::to_string(&self.args()).unwrap();
            let parameters: serde_json::Value = serde_json::from_str(&parameters).unwrap();

            Ok(query
                .bind(self.hash().as_ref())
                .bind(self.block_num())
                .bind(self.ext_module())
                .bind(self.ext_call())
                .bind(Some(parameters))
                .bind(self.index() as u32)
                .bind(0 as u32))
        }
    }
}
