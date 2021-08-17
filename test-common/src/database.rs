// Copyright 2018-2021 Parity Technologies (UK) Ltd.
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

//! Common primitives & routines for SQL tests

use async_std::task;
use std::env;
use once_cell::sync::Lazy;

pub static DATABASE_URL: Lazy<String> = Lazy::new(|| env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set to run tests!"));
pub const DUMMY_HASH: [u8; 2] = [0x13, 0x37];

pub static PG_POOL: Lazy<sqlx::PgPool> = Lazy::new(|| {
	task::block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(4)
            .max_connections(8)
            .idle_timeout(std::time::Duration::from_millis(3600))
            .connect(&DATABASE_URL)
            .await
            .expect("Couldn't initialize postgres pool for tests");
        sqlx::migrate!("../substrate-archive/src/migrations/").run(&mut pool.acquire().await.unwrap()).await.unwrap();
        pool
	})
});


pub fn insert_dummy_sql() {
    task::block_on(async {
        sqlx::query(
            r#"
                INSERT INTO metadata (version, meta)
                VALUES($1, $2)
            "#,
        )
        .bind(0)
        .bind(&DUMMY_HASH[0..2])
        .execute(&*PG_POOL)
        .await
        .unwrap();

        // insert a dummy block
        sqlx::query(
                "
                    INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec)
                    VALUES($1, $2, $3, $4, $5, $6, $7, $8)
                ")
                .bind(&DUMMY_HASH[0..2])
                .bind(&DUMMY_HASH[0..2])
                .bind(0)
                .bind(&DUMMY_HASH[0..2])
                .bind(&DUMMY_HASH[0..2])
                .bind(&DUMMY_HASH[0..2])
                .bind(&DUMMY_HASH[0..2])
                .bind(0)
                .execute(&*PG_POOL)
                .await
                .expect("INSERT");
    });
}

