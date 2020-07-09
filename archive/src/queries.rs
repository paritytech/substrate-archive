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

use crate::{
    error::{ArchiveResult, Error as ArchiveError},
    sql_block_builder::SqlBlock,
};
use futures::Stream;
use sp_runtime::traits::Block as BlockT;

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct BlockNumSeries {
    pub generate_series: i32,
}

/// get missing blocks from relational database as a stream
pub(crate) fn missing_blocks_stream(
    conn: &mut sqlx::PgConnection,
) -> impl Stream<Item = Result<(i32,), sqlx::Error>> + Send + '_ {
    sqlx::query_as::<_, (i32,)>(
        "SELECT generate_series
        FROM (SELECT 0 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
        WHERE
        NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
        ORDER BY generate_series ASC
        ",
    )
    .fetch(conn)
}

/// get missing blocks from relational database
#[allow(unused)]
pub(crate) async fn missing_blocks(conn: &sqlx::PgPool) -> ArchiveResult<Vec<u32>> {
    Ok(sqlx::query_as::<_, (i32,)>(
        "SELECT generate_series
        FROM (SELECT 0 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
        WHERE
        NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
        ORDER BY generate_series ASC
        ",
    )
    .fetch_all(conn)
    .await?
    .iter()
    .map(|t| t.0 as u32)
    .collect())
}

/// Will get blocks such that they exist in the `blocks` table but they
/// do not exist in the `storage` table
/// blocks are ordered by spec version
/// this is so the runtime code can be kept in cache without
/// constantly switching between runtime versions if the blocks will be executed
pub(crate) async fn blocks_storage_intersection(
    pool: &sqlx::PgPool,
) -> Result<Vec<SqlBlock>, ArchiveError> {
    sqlx::query_as(
        "SELECT *
        FROM blocks
        WHERE NOT EXISTS (SELECT * FROM storage WHERE storage.block_num = blocks.block_num)
        ORDER BY blocks.spec",
    )
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) async fn get_full_block(
    pool: &sqlx::PgPool,
    block_num: u32,
) -> Result<SqlBlock, ArchiveError> {
    sqlx::query_as(
        "
        SELECT parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec
        FROM blocks
        WHERE block_num = $1
        ",
    )
    .bind(block_num as i32)
    .fetch_one(pool)
    .await
    .map_err(Into::into)
}

pub(crate) async fn blocks_count(pool: &sqlx::PgPool) -> Result<u64, ArchiveError> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blocks")
        .fetch_one(pool)
        .await
        .map_err(ArchiveError::from)?;
    Ok(row.0 as u64)
}

/// check if a runtime versioned metadata exists in the database
pub(crate) async fn check_if_meta_exists(
    spec: u32,
    pool: &sqlx::PgPool,
) -> Result<bool, ArchiveError> {
    let row: (bool,) =
        sqlx::query_as(r#"SELECT EXISTS(SELECT version FROM metadata WHERE version=$1)"#)
            .bind(spec)
            .fetch_one(pool)
            .await?;
    Ok(row.0)
}

pub(crate) async fn contains_block<B: BlockT>(
    hash: B::Hash,
    pool: &sqlx::PgPool,
) -> Result<bool, ArchiveError> {
    let hash = hash.as_ref();
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

#[cfg(test)]
mod tests {
    //! Must be connected to a postgres database
    use super::*;
    // use diesel::test_transaction;
}
