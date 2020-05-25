// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

use anyhow::Result;
use sqlx::{postgres::PgQueryAs as _, PgConnection};

pub(crate) async fn block_count(
    pool: &sqlx::Pool<PgConnection>,
) -> Result<u32> {
    let row: (i64, ) = sqlx::query_as(
        "SELECT COUNT(*) FROM blocks",
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0 as u32)
}

pub(crate) async fn max_block(pool: &sqlx::Pool<PgConnection>,
) -> Result<u32> {
    let row: (i64, ) = sqlx::query_as(
        "SELECT block_num FROM blocks WHERE block_num = (SELECT MAX(block_num) FROM blocks)"
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0 as u32)
}

pub(crate) async fn extrinsic_count(
    pool: &sqlx::Pool<PgConnection>,
) -> Result<u32> {
    let row: (i64, ) = sqlx::query_as(
        "SELECT COUNT(*) FROM extrinsics"
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0 as u32)
}

pub(crate) async fn storage_count(
    pool: &sqlx::Pool<PgConnection>,
) -> Result<u32> {
    let row: (i64, ) = sqlx::query_as(
        "SELECT COUNT(*) FROM storage"
    )
    .fetch_one(pool)
    .await?;
    Ok(row.0 as u32)
}

pub(crate) async fn get_max_storage(
    pool: &sqlx::Pool<PgConnection>,
) -> Result<(u32, Vec<u8>)> {
    let row: (i64, Vec<u8>) =
        sqlx::query_as(
            "SELECT block_num, hash FROM storage WHERE block_num = (SELECT MAX(block_num) FROM storage)"
        )
        .fetch_one(pool)
        .await?;
    Ok((row.0 as u32, row.1))
}



