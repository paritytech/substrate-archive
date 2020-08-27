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
mod types;
mod util;
mod tasks;

pub use actors::System;
pub use archive::{ArchiveBuilder, ArchiveConfig};
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
use test::{PG_POOL, DATABASE_URL, initialize};

#[cfg(test)]
mod test {
    use std::sync::Once;
    use once_cell::sync::Lazy;
    
    pub static DATABASE_URL: Lazy<String> = Lazy::new(|| {
        dotenv::var("DATABASE_URL").expect("TEST_DATABASE_URL must be set to run tests!")
    });

    pub static PG_POOL: Lazy<sqlx::PgPool> = Lazy::new(|| {
        smol::block_on(sqlx::postgres::PgPoolOptions::new()
            .min_connections(4)
            .max_connections(8)
            .idle_timeout(std::time::Duration::from_millis(3600))
            .connect(&DATABASE_URL)
        ).expect("Couldn't initialize postgres pool for tests")
    });
    
    static INIT: Once = Once::new();
    pub fn initialize() {
        INIT.call_once(|| {
            pretty_env_logger::init();
            let url: &str = &DATABASE_URL;
            smol::block_on(crate::migrations::migrate(url)).unwrap();
        });
    }
}

