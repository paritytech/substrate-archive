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
use futures::{
    stream::{StreamExt, TryStreamExt},
    Stream,
};
use sqlx::{postgres::PgQueryAs as _, prelude::Cursor, PgConnection, QueryAs as _};

#[derive(sqlx::FromRow)]
pub struct Block {
    pub block_num: u32,
}

/// get missing blocks from relational database
pub(crate) async fn missing_blocks(
    latest: Option<u32>,
    pool: &sqlx::Pool<PgConnection>,
) -> impl Stream<Item = Result<Block, ArchiveError>> + '_ {
    if let Some(latest) = latest {
        sqlx::query_as(
            "SELECT generate_series
            FROM generate_series('0'::bigint, '{}'::bigint)
            WHERE
            NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)",
        )
        .fetch(pool)
        .map_err(Into::into)
    } else {
        sqlx::query_as(
            "SELECT generate_series
            FROM (SELECT 0 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
            WHERE
            NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)",
        )
        .fetch(pool)
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    //! Must be connected to a postgres database
    use super::*;
    // use diesel::test_transaction;
}
