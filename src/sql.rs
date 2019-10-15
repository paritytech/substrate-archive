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
    sql_types::{Integer, Array}
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
    fn generate_series(start: Integer, stop: Integer) -> Array<Integer>
}

//
//
// SELECT id
// FROM  (SELECT min(id) AS a, max(id) AS z FROM numbers) x, generate_series(a, z) id
// LEFT   JOIN numbers n1 USING (id)
// WHERE  n1.id IS NULL;
//
// this should work
//
// SELECT block
// FROM (SELECT min(block) AS a, max(block) AS z FROM blocks) x, generate_series(a, z) blocks
// LEFT JOIN hash n1 USING (blocks)
// WHERE n1.hash IS NULL;
//
//
/*
pub(crate) fn get_missing_blocks<T>(db: &AsyncDiesel<T>)
                                    -> impl Future<Item = Vec<u64>, Error = ArchiveError>
where
    T: System + diesel::Connection
{
    use crate::database::schema::blocks::dsl::{blocks, block, hash};

    db.run(move |conn| {
        let min_block = min(block);
        let max_block = max(block);
        generate_series(min_block, max_block)
            .select(block)
            .left_join(blocks.hash)
            .filter(hash == None)
            .load::<Vec<i64>>(&conn)
            .expect("Error loading missing")
    })
}
*/
