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

//! Common Sql queries on Archive Database abstracted into rust functions

use crate::{error::Error as ArchiveError, sql_block_builder::SqlBlock};
use sqlx::{postgres::PgQueryAs as _, PgConnection};

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct BlockNumSeries {
    pub generate_series: i32,
}

/// get missing blocks from relational database
pub(crate) async fn missing_blocks(
    pool: &sqlx::PgPool,
) -> Result<Vec<BlockNumSeries>, ArchiveError> {
    sqlx::query_as(
        "SELECT generate_series
        FROM (SELECT 0 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
        WHERE
        NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
        LIMIT 10000",
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

pub(crate) async fn missing_storage(
    pool: &sqlx::PgPool,
) -> Result<Vec<BlockNumSeries>, ArchiveError> {
    sqlx::query_as(
        "SELECT generate_series
        FROM (SELECT 0 as a, max(block_num) as z FROM storage) x, generate_series(a, z)
        WHERE
        NOT EXISTS(SELECT id FROM storage WHERE block_num = generate_series)
        LIMIT 10000",
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

/// Will get blocks such that they exist in the `blocks` table but they
/// do not exist in the `storage` table
pub(crate) async fn blocks_storage_intersection(
    pool: &sqlx::PgPool,
) -> Result<Vec<SqlBlock>, ArchiveError> {
    sqlx::query_as(
       "SELECT a.parent_hash, a.hash, a.block_num, a.state_root, a.extrinsics_root, a.digest, a.ext, a.spec
        FROM blocks AS a
        LEFT JOIN storage b ON b.block_num = a.block_num
        WHERE b.block_num IS NULL"
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

/// Will get blocks such that they exist in the `blocks` table but they
/// do not exist in the `storage` table
pub(crate) async fn blocks_storage_intersection_count(
    pool: &sqlx::PgPool,
) -> Result<u64, ArchiveError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
        FROM blocks AS a
        LEFT JOIN storage b ON b.block_num = a.block_num
        WHERE b.block_num IS NULL",
    )
    .fetch_one(pool)
    .await
    .map_err(ArchiveError::from)?;
    Ok(row.0 as u64)
}

pub(crate) async fn missing_blocks_min_max(
    pool: &sqlx::PgPool,
    min: u32,
    max: u32,
) -> Result<Vec<BlockNumSeries>, ArchiveError> {
    sqlx::query_as(
        "SELECT generate_series
         FROM (SELECT $1 as a, $2 as z FROM blocks) x, generate_series(a, z)
         WHERE NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)",
    )
    .bind(min as i32)
    .bind(max as i32)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

/// check if a runtime versioned metadata exists in the database
pub(crate) async fn check_if_meta_exists(
    spec: u32,
    pool: &sqlx::PgPool,
) -> Result<bool, ArchiveError> {
    let row: (bool,) = sqlx::query_as(r#"SELECT EXISTS(SELECT 1 FROM metadata WHERE version=$1)"#)
        .bind(spec)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

pub(crate) async fn check_if_block_exists(
    hash: &[u8],
    pool: &sqlx::PgPool,
) -> Result<bool, ArchiveError> {
    let row: (bool,) = sqlx::query_as(r#"SELECT EXISTS(SELECT 1 FROM blocks WHERE hash=$1)"#)
        .bind(hash)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}

#[derive(sqlx::FromRow, Debug)]
pub struct Version {
    pub version: i32,
}

pub(crate) async fn get_versions(pool: &sqlx::PgPool) -> Result<Vec<Version>, ArchiveError> {
    sqlx::query_as::<_, Version>("SELECT version FROM metadata")
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

pub(crate) async fn get_max_storage(pool: &sqlx::PgPool) -> Result<(u32, Vec<u8>), ArchiveError> {
    let row: (i32, Vec<u8>) =
        sqlx::query_as(r#"SELECT block_num, hash FROM storage WHERE block_num = (SELECT MAX(block_num) FROM storage)"#)
        .fetch_one(pool)
        .await?;
    Ok((row.0 as u32, row.1))
}

pub(crate) async fn get_max_block_num(pool: &sqlx::PgPool) -> Result<(u32, Vec<u8>), ArchiveError> {
    let row: (i32, Vec<u8>) =
        sqlx::query_as(r#"SELECT block_num, hash FROM blocks WHERE block_num = (SELECT MAX(block_num) FROM blocks)"#)
        .fetch_one(pool)
        .await?;
    Ok((row.0 as u32, row.1))
}

/// checks if blocks table has anything in it
pub(crate) async fn are_blocks_empty(pool: &sqlx::PgPool) -> Result<bool, ArchiveError> {
    let row: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) from blocks"#)
        .fetch_one(pool)
        .await?;
    Ok(!(row.0 > 0))
}

#[cfg(test)]
mod tests {
    //! Must be connected to a postgres database
    use super::*;
    // use diesel::test_transaction;
}
