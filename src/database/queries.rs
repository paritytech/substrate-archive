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

use super::BlockModel;
use crate::error::{Error, Result};
use futures::{stream::TryStreamExt, Stream};
use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};
use sp_runtime::traits::Block as BlockT;
use sqlx::PgConnection;

/// get missing blocks from relational database as a stream
#[allow(unused)]
pub(crate) fn missing_blocks_stream(
    conn: &mut PgConnection,
) -> impl Stream<Item = Result<(i32,)>> + Send + '_ {
    sqlx::query_as::<_, (i32,)>(
        "SELECT generate_series
        FROM (SELECT 0 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
        WHERE
        NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
        ORDER BY generate_series ASC
        ",
    )
    .fetch(conn)
    .map_err(Error::from)
}

/// get missing blocks from relational database
#[allow(unused)]
pub(crate) async fn missing_blocks(conn: &mut PgConnection) -> Result<Vec<u32>> {
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

pub(crate) async fn missing_blocks_min_max(
    conn: &mut PgConnection,
    min: u32,
) -> Result<HashSet<u32>> {
    Ok(sqlx::query_as::<_, (i32,)>(
        "SELECT generate_series
        FROM (SELECT $1 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
        WHERE
        NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
        ORDER BY generate_series ASC
        ",
    )
    .bind(min as i32)
    .fetch_all(conn)
    .await?
    .iter()
    .map(|t| t.0 as u32)
    .collect())
}

pub(crate) async fn max_block(conn: &mut PgConnection) -> Result<Option<u32>> {
    let row = sqlx::query_as::<_, (Option<i32>,)>("SELECT MAX(block_num) FROM blocks")
        .fetch_one(conn)
        .await?;

    Ok(row.0.map(|v| v as u32))
}

/// Will get blocks such that they exist in the `blocks` table but they
/// do not exist in the `storage` table
/// blocks are ordered by spec version
///
/// # Returns full blocks
pub(crate) async fn blocks_storage_intersection(
    conn: &mut sqlx::PgConnection,
) -> Result<Vec<BlockModel>> {
    sqlx::query_as(
        "SELECT *
        FROM blocks
        WHERE NOT EXISTS (SELECT * FROM storage WHERE storage.block_num = blocks.block_num)
        AND blocks.block_num != 0
        ORDER BY blocks.spec",
    )
    .fetch_all(conn)
    .await
    .map_err(Into::into)
}

pub(crate) async fn get_full_block_by_id(
    conn: &mut sqlx::PgConnection,
    id: i32,
) -> Result<BlockModel> {
    sqlx::query_as(
        "
        SELECT id, parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec
        FROM blocks
        WHERE id = $1
        ",
    )
    .bind(id)
    .fetch_one(conn)
    .await
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) async fn get_full_block_by_num(
    conn: &mut sqlx::PgConnection,
    block_num: u32,
) -> Result<BlockModel> {
    sqlx::query_as(
        "
        SELECT parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec
        FROM blocks
        WHERE block_num = $1
        ",
    )
    .bind(block_num as i32)
    .fetch_one(conn)
    .await
    .map_err(Into::into)
}

/// check if a runtime versioned metadata exists in the database
pub(crate) async fn check_if_meta_exists(spec: u32, conn: &mut PgConnection) -> Result<bool> {
    let row: (bool,) =
        sqlx::query_as(r#"SELECT EXISTS(SELECT version FROM metadata WHERE version = $1)"#)
            .bind(spec)
            .fetch_one(conn)
            .await?;
    Ok(row.0)
}

pub(crate) async fn has_block<B: BlockT>(hash: B::Hash, conn: &mut PgConnection) -> Result<bool> {
    let hash = hash.as_ref();
    let row: (bool,) = sqlx::query_as(r#"SELECT EXISTS(SELECT 1 FROM blocks WHERE hash = $1)"#)
        .bind(hash)
        .fetch_one(conn)
        .await?;
    Ok(row.0)
}

/// Returns a list of block_numbers, out of the passed-in blocknumbers, which exist in the database
pub(crate) async fn has_blocks<B: BlockT>(
    nums: &[u32],
    conn: &mut PgConnection,
) -> Result<Vec<u32>> {
    let query = String::from("SELECT block_num FROM blocks WHERE block_num = ANY ($1)");
    let row = sqlx::query_as::<_, (i32,)>(query.as_str())
        .bind(nums)
        .fetch_all(conn)
        .await?;
    Ok(row.into_iter().map(|r| r.0 as u32).collect())
}

pub(crate) async fn get_versions(conn: &mut PgConnection) -> Result<Vec<u32>> {
    let rows = sqlx::query_as::<_, (i32,)>("SELECT version FROM metadata")
        .fetch_all(conn)
        .await?;
    Ok(rows.into_iter().map(|r| r.0 as u32).collect())
}

pub(crate) async fn get_all_blocks<B: BlockT + DeserializeOwned>(
    conn: &mut PgConnection,
) -> Result<impl Iterator<Item = Result<B>>> {
    let blocks = sqlx::query_as::<_, (Vec<u8>,)>(
        "SELECT data FROM _background_tasks WHERE job_type = 'execute_block'",
    )
    .fetch_all(conn)
    .await?;

    // temporary struct to deserialize job
    #[derive(Deserialize)]
    struct JobIn<BL: BlockT> {
        block: BL,
    }
    Ok(blocks.into_iter().map(|r| {
        let b: JobIn<B> = rmp_serde::from_read(r.0.as_slice())?;
        Ok(b.block)
    }))
}

#[cfg(test)]
mod tests {
    //! Must be connected to a postgres database
    use super::*;
    // use diesel::test_transaction;
}
