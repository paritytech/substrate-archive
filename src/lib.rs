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

// #![allow(warnings)]
// #![forbid(unsafe_code)]

mod actors;
pub mod archive;
pub mod backend;
mod database;
mod error;
mod migrations;
// mod rpc;
// #[cfg(test)]
// mod simple_db;
mod sql_block_builder;
mod tasks;
mod types;
mod util;

pub use actors::System;
pub use archive::Builder as ArchiveBuilder;
pub use database::queries;
pub use error::Error;
pub use migrations::MigrationConfig;
pub use types::Archive;

#[cfg(feature = "logging")]
pub use util::init_logger;

// Re-Exports
pub use sc_executor::native_executor_instance;
pub use sp_blockchain::Error as BlockchainError;
pub use sp_runtime::MultiSignature;
pub mod chain_traits {
    //! Traits defining functions on the client needed for indexing
    pub use sc_client_api::client::BlockBackend;
    pub use sp_blockchain::{HeaderBackend, HeaderMetadata};
    pub use sp_runtime::traits::{BlakeTwo256, Block, IdentifyAccount, Verify};
}

#[derive(Debug, Clone)]
pub struct TaskExecutor;

impl futures::task::Spawn for TaskExecutor {
    fn spawn_obj(
        &self,
        future: futures::task::FutureObj<'static, ()>,
    ) -> Result<(), futures::task::SpawnError> {
        smol::Task::spawn(future).detach();
        Ok(())
    }
}

impl sp_core::traits::SpawnNamed for TaskExecutor {
    fn spawn(
        &self,
        _: &'static str,
        fut: std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + 'static>>,
    ) {
        smol::Task::spawn(fut).detach()
    }

    fn spawn_blocking(
        &self,
        _: &'static str,
        fut: std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send + 'static>>,
    ) {
        smol::Task::spawn(async move { smol::unblock!(fut) }).detach();
    }
}

#[cfg(test)]
use test::{initialize, TestGuard, DATABASE_URL, DUMMY_HASH, PG_POOL};

#[cfg(test)]
mod test {
    use once_cell::sync::Lazy;
    use sqlx::prelude::*;
    use std::sync::{Mutex, MutexGuard, Once};

    pub static DATABASE_URL: Lazy<String> = Lazy::new(|| {
        dotenv::var("DATABASE_URL").expect("TEST_DATABASE_URL must be set to run tests!")
    });

    pub const DUMMY_HASH: [u8; 2] = [0x13, 0x37];

    pub static PG_POOL: Lazy<sqlx::PgPool> = Lazy::new(|| {
        smol::block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .min_connections(4)
                .max_connections(8)
                .idle_timeout(std::time::Duration::from_millis(3600))
                .connect(&DATABASE_URL)
                .await
                .expect("Couldn't initialize postgres pool for tests");
            pool
        })
    });

    static INIT: Once = Once::new();
    pub fn initialize() {
        INIT.call_once(|| {
            pretty_env_logger::init();
            let url: &str = &DATABASE_URL;
            smol::block_on(async {
                crate::migrations::migrate(url).await.unwrap();
            });
        });
    }

    static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    pub struct TestGuard<'a>(MutexGuard<'a, ()>);
    impl<'a> TestGuard<'a> {
        pub(crate) fn lock() -> Self {
            let guard = TestGuard(TEST_MUTEX.lock().expect("Test mutex panicked"));
            smol::block_on(async {
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
            guard
        }
    }

    impl<'a> Drop for TestGuard<'a> {
        fn drop(&mut self) {
            smol::block_on(async move {
                let mut conn = crate::PG_POOL.acquire().await.unwrap();
                conn.execute(
                    "
                    TRUNCATE TABLE metadata CASCADE;
                    TRUNCATE TABLE storage CASCADE;
                    TRUNCATE TABLE blocks CASCADE;
                    TRUNCATE TABLE frame_system CASCADE;
                    TRUNCATE TABLE _background_tasks
                    ",
                )
                .await
                .unwrap();
            });
        }
    }
}
