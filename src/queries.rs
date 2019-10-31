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

pub(crate) fn missing_blocks() -> diesel::query_builder::SqlQuery {
    let query = "\
SELECT generate_series
  FROM (SELECT 0 as a, max(block_num) as z FROM blocks) x, generate_series(a, z)
WHERE
  NOT EXISTS(SELECT id FROM blocks WHERE block_num = generate_series)
";
    diesel::sql_query(query)
}

pub(crate) fn missing_timestamp() -> diesel::query_builder::SqlQuery {
    let query = "\
SELECT hash
FROM blocks
WHERE block_num > 0 AND time IS NULL
";
    diesel::sql_query(query) // block_num must be > 0 because genesis block will not have timestamp
}

// Get the latest block in the database
// this might not be up-to-date right as the node starts,
// but will soon start collecting the latest heads
#[allow(dead_code)]
pub(crate) fn head() -> diesel::query_builder::SqlQuery {
    unimplemented!()
}


#[cfg(test)]
mod tests {
    //! Must be connected to a postgres database
    use super::*;
    // use diesel::test_transaction;

}
