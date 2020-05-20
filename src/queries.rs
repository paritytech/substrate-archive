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

use crate::error::Error as ArchiveError;
use crate::types::Substrate;
use futures::{
    stream::{StreamExt, TryStreamExt},
    Future, Stream,
};
use sqlx::{
    postgres::{PgQueryAs as _, PgRow},
    prelude::Cursor,
    PgConnection, QueryAs as _,
};

#[derive(sqlx::FromRow, Debug)]
pub struct Block {
    pub generate_series: i64,
}

/// get missing blocks from relational database
pub(crate) async fn missing_blocks(
    pool: &sqlx::Pool<PgConnection>,
) -> Result<Vec<Block>, ArchiveError> {
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

/// check if a runtime versioned metadata exists in the database
pub(crate) async fn check_if_meta_exists(
    spec: u32,
    pool: &sqlx::Pool<PgConnection>,
) -> Result<bool, ArchiveError> {
    let row: (bool,) = sqlx::query_as(r#"SELECT EXISTS(SELECT 1 FROM metadata WHERE version=$1)"#)
        .bind(spec)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}
#[derive(sqlx::FromRow, Debug)]
pub struct Version {
    pub version: i32,
}

pub(crate) async fn get_versions(
    pool: &sqlx::Pool<PgConnection>,
) -> Result<Vec<Version>, ArchiveError> {
    sqlx::query_as::<_, Version>("SELECT version FROM metadata")
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

pub(crate) async fn get_max_storage(
    pool: &sqlx::Pool<PgConnection>
) -> Result<(u32, Vec<u8>), ArchiveError> {
    let row: (i64, Vec<u8>) =
        sqlx::query_as(r#"SELECT block_num, hash FROM storage WHERE block_num = (SELECT MAX(block_num) FROM storage)"#)
        .fetch_one(pool)
        .await?;
    Ok((row.0 as u32, row.1))
}

pub(crate) async fn get_max_block_num(
    pool: &sqlx::Pool<PgConnection>
) -> Result<(u32, Vec<u8>), ArchiveError> {
    let row: (i64, Vec<u8>) =
        sqlx::query_as(r#"SELECT block_num, hash FROM blocks WHERE block_num = (SELECT MAX(block_num) FROM blocks)"#)
        .fetch_one(pool)
        .await?;
    Ok((row.0 as u32, row.1))
}

pub(crate) async fn is_blocks_empty(pool: &sqlx::Pool<PgConnection>) -> Result<bool, ArchiveError> {

    let row: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) from blocks"#)
        .fetch_one(pool)
        .await?;
    Ok(! (row.0 > 0))
}

pub(crate) async fn is_storage_empty(pool: &sqlx::Pool<PgConnection>) -> Result<bool, ArchiveError> {
    let row: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) from storage"#)
        .fetch_one(pool)
        .await?;
    Ok( !(row.0 > 0))
}

pub(crate) async check_if_storage_exists(pool: &sqlx::Pool<PgConnection>) -> Result<bool, ArchiveError> {
    unimplemented!();
}


#[cfg(test)]
mod tests {
    //! Must be connected to a postgres database
    use super::*;
    // use diesel::test_transaction;
}
