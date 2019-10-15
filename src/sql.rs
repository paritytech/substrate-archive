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

use diesel::{
    dsl::{min, max},
    sql_types::{BigInt, Array},
    QueryDsl, ExpressionMethods,
};
use futures::Future;

use crate::{
    types::System,
    database::db_middleware::AsyncDiesel,
    error::Error as ArchiveError
};

diesel::sql_function!{
    /// The Generate Series SQL Function
    /// Generate a series of values, from start to stop with a step size of one
    /// https://www.postgresql.org/docs/9.1/functions-srf.html
    /// only supports the simplest variation (no control over step or interval)
    fn generate_series(start: BigInt, stop: BigInt) -> Array<BigInt>
}


const MISSING_BLOCKS: &'static str = "\
SELECT
  generate_series FROM GENERATE_SERIES(
    0, (select max(block_num) from blocks)
  )
WHERE
  NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
";

/*
SELECT
  generate_series FROM GENERATE_SERIES(
    0, (select max(block_num) from blocks)
  )
WHERE
  NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
*/

pub(crate) fn missing_blocks() -> diesel::query_builder::SqlQuery
{
    use crate::database::schema::blocks::dsl::{blocks, block_num, hash, id};
    // use diesel::dsl::{exists, not};
    diesel::sql_query(MISSING_BLOCKS)

/*
    let generate_series = generate_series(0, max(block_num));
    blocks.filter(not(exists(
        blocks.select(id).filter(block_num.eq(generate_series.select(block_num)))
    )))
*/
}
/*
pub(crate) fn block_exists() -> {


}
*/
