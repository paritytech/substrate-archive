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

use super::models::StorageModel;
use crate::error::ArchiveResult;
use crate::types::{Block, Metadata, Storage};
use sp_runtime::traits::{Block as BlockT, NumberFor};
use sqlx::Postgres;

pub trait SuperTrait<'a>: PrepareSql<'a> + PrepareBatchSql<'a> {}

impl<'a, T> SuperTrait<'a> for T where T: PrepareSql<'a> + PrepareBatchSql<'a> {}

pub trait BindAll<'a> {
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
}

pub trait PrepareSql<'a> {
    /// prepare a query for insertion
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
}

pub trait PrepareBatchSql<'a> {
    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>>;
    fn build_sql(&self, rows: Option<u32>) -> String;
}

impl<'a, B> PrepareSql<'a> for Block<B>
where
    B: BlockT,
    NumberFor<B>: From<u32>,
    NumberFor<B>: Into<u32>,
{
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let query = sqlx::query(
            r#"
    INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec)
    VALUES($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT DO NOTHING
    "#,
        );

        self.bind_all_arguments(query)
    }
}

impl<'a, B> PrepareBatchSql<'a> for Vec<Block<B>>
where
    B: BlockT,
    NumberFor<B>: From<u32>,
    NumberFor<B>: Into<u32>,
{
    fn build_sql(&self, _rows: Option<u32>) -> String {
        let stmt = format!(
            r#"
INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec)
VALUES {}
ON CONFLICT DO NOTHING
"#,
            build_batch_insert(self.len(), 8)
        );
        stmt
    }

    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        Ok(self
            .iter()
            .fold(sqlx::query(sql), |q, block| block.bind_all_arguments(q).unwrap()))
    }
}

impl<'a> PrepareSql<'a> for Metadata {
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let query = sqlx::query(
            r#"
INSERT INTO metadata (version, meta)
VALUES($1, $2)
ON CONFLICT DO NOTHING
"#,
        );
        self.bind_all_arguments(query)
    }
}

impl<'a, B: BlockT> PrepareSql<'a> for StorageModel<B> {
    fn single_insert(&self) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let query = sqlx::query(
            r#"
INSERT INTO storage (block_num, hash, is_full, key, storage)
VALUES (#1, $2, $3, $4, $5)
ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
hash = EXCLUDED.hash,
key = EXCLUDED.key,
storage = EXCLUDED.storage,
is_full = EXCLUDED.is_full
"#,
        );
        self.bind_all_arguments(query)
    }
}

impl<'a, B: BlockT> PrepareBatchSql<'a> for Vec<StorageModel<B>> {
    fn build_sql(&self, rows: Option<u32>) -> String {
        if let Some(r) = rows {
            format!(
                r#"
    INSERT INTO storage (block_num, hash, is_full, key, storage)
    VALUES {}
    ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
    hash = EXCLUDED.hash,
    key = EXCLUDED.key,
    storage = EXCLUDED.storage,
    is_full = EXCLUDED.is_full
    "#,
                build_batch_insert(r as usize, 5)
            )
        } else {
            format!(
                r#"
            INSERT INTO storage (block_num, hash, is_full, key, storage)
            VALUES {}
            ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
            hash = EXCLUDED.hash,
            key = EXCLUDED.key,
            storage = EXCLUDED.storage,
            is_full = EXCLUDED.is_full
            "#,
                build_batch_insert(self.len(), 5)
            )
        }
    }

    fn batch_insert(&self, sql: &'a str) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        Ok(self.iter().fold(sqlx::query(sql), |q, storg| {
            storg
                .bind_all_arguments(q)
                .expect("Could not bind storage arguments")
        }))
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
