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
use sqlx::postgres::PgArguments;
use subxt::system::System;

pub trait SuperTrait<'a>: PrepareSql<'a> + PrepareBatchSql<'a> + GetArguments
{}

pub trait GetArguments {
    fn get_arguments(&self) -> ArchiveResult<PgArguments>;
}

impl<'a, T> SuperTrait<'a> for T
where
    T: PrepareSql<'a> + PrepareBatchSql<'a> + GetArguments
{}

pub trait BindAll<'a> {
    fn bind_all_arguments(&self, query: sqlx::Query<'a, Postgres>) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
}

pub trait PrepareSql<'a> {
    /// prepare a query for insertion
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
}

pub trait PrepareBatchSql<'a> {
    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
    fn build_sql(&self) -> String;
}

impl<'a, T> PrepareSql<'a> for Block<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: From<u32>,
    <T as System>::BlockNumber: Into<u32>,
{
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {

        let query =
            sqlx::query(
                r#"
    INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root)
    VALUES($1, $2, $3, $4, $5)
    "#);

        self.bind_all_arguments(query)
    }
}

impl<'a, T> PrepareBatchSql<'a> for Vec<Block<T>>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: From<u32>,
<T as System>::BlockNumber: Into<u32>,
{

    fn build_sql(&self) -> String {
        format!(
            r#"
INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root)
VALUES {}
"#,
             build_batch_insert(self.len(), 5)
        )
    }

    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>> {

        Ok(
            self.iter()
                .fold(sqlx::query(sql), |q, block| {
                    block.bind_all_arguments(q).unwrap()
            })
        )
    }
}

impl<'a, T> PrepareSql<'a> for SignedExtrinsic<T>
where
    T: Substrate + Send + Sync,
{
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let query =
            sqlx::query(r#"
INSERT INTO signed_extrinsics (hash, block_num, from_addr, module, call, parameters, tx_index, signature, extra, transaction_version)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
"#);
        self.bind_all_arguments(query)
    }
}

impl<'a, T> PrepareBatchSql<'a> for Vec<SignedExtrinsic<T>>
where
    T: Substrate + Send + Sync,
{
    fn build_sql(&self) -> String {
        format!(
            r#"
INSERT INTO signed_extrinsics (hash, block_num, from_addr, module, call, parameters, tx_index, signature, extra, transaction_version)
VALUES {}
"#,
        build_batch_insert(self.len(), 10)
        )
    }

    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>> {

        Ok(
            self.iter()
                .fold(sqlx::query(sql), |q, ext| {
                    // let arguments = ext.get_arguments().unwrap();
                    ext.bind_all_arguments(q).unwrap()
                })
        )
    }
}


impl<'a, T> PrepareSql<'a> for Inherent<T>
where
    T: Substrate + Send + Sync,
{
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        // log::info!("================== EXT ===============");
        // log::info!("Preparing Inherent {}", self.index());
        // log::info!("Hash {:?}", self.hash().as_ref());
        // log::info!("Num: {}", self.block_num());
        // log::info!("Module: {}", self.ext_module());
        // log::info!("Call: {}", self.ext_call());
        // log::info!("Parameters: {:?}", parameters);
        // log::info!("Index: {}", self.index() as u32);
        // log::info!("Transaction Version {}", 0 as u32);
        // log::info!("================== EXT ===============");

        let query =
            sqlx::query(
            r#"
INSERT INTO inherents (hash, block_num, module, call, parameters, in_index, transaction_version)
VALUES($1, $2, $3, $4, $5, $6, $7)
"#,
            );
        self.bind_all_arguments(query)
    }
}

impl<'a, T> PrepareBatchSql<'a> for Vec<Inherent<T>>
where
    T: Substrate + Send + Sync,
{

    fn build_sql(&self) -> String {
        let stmt = format!(
            r#"
INSERT INTO inherents (hash, block_num, module, call, parameters, in_index, transaction_version)
VALUES {}
"#,
            build_batch_insert(self.len(), 7)
        );

        log::info!("INHERENT SQL STATEMENT: {}", stmt);
        stmt
    }

    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>> {

        Ok(
            self.iter()
                .fold(sqlx::query(sql), |q, ext| {
                    ext.bind_all_arguments(q).unwrap()
                })
        )
    }
}

/// Create a batch insert statement
///
/// This code created by @mehcode
/// https://discordapp.com/channels/665528275556106240/665528275556106243/694835667401703444
fn build_batch_insert(rows: usize, columns: usize) -> String {
    use itertools::Itertools;
    (0..rows)
        .format_with(",", |i, f| {
            f(&format_args!(
                "({})",
                (1..=columns).format_with(",", |j, f| f(&format_args!("${}", j + (i * columns))))
            ))
        })
        .to_string()
}
