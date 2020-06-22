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

//! Main entrypoint for substrate-archive. `init` will start all actors and begin indexing the
//! chain defined with the passed-in Client and URL.

mod generators;
mod scheduler;
mod workers;

use super::{
    backend::{ApiAccess, BlockBroker, ReadOnlyBackend, ThreadedBlockExecutor},
    error::Error as ArchiveError,
    types::{NotSignedBlock, Substrate, System},
};
use bastion::prelude::*;
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sqlx::postgres::PgPool;
use std::sync::Arc;

/// Context that every actor may use
#[derive(Clone)]
pub struct ActorContext<T: Substrate + Send + Sync> {
    backend: Arc<ReadOnlyBackend<NotSignedBlock<T>>>,
    broker: BlockBroker<NotSignedBlock<T>>,
    rpc_url: String,
    pool: sqlx::PgPool,
}

impl<T: Substrate + Send + Sync> ActorContext<T> {
    pub fn new(
        backend: Arc<ReadOnlyBackend<NotSignedBlock<T>>>,
        broker: BlockBroker<NotSignedBlock<T>>,
        rpc_url: String,
        pool: sqlx::PgPool,
    ) -> Self {
        Self {
            backend,
            broker,
            rpc_url,
            pool,
        }
    }

    pub fn backend(&self) -> &Arc<ReadOnlyBackend<NotSignedBlock<T>>> {
        &self.backend
    }

    pub fn rpc_url(&self) -> &str {
        self.rpc_url.as_str()
    }

    pub fn pool(&self) -> sqlx::PgPool {
        self.pool.clone()
    }

    pub fn broker(&self) -> &BlockBroker<NotSignedBlock<T>> {
        &self.broker
    }
}

/// Main entrypoint for substrate-archive.
/// Deals with starting and stopping the Archive Runtime
/// # Examples
/// ```
///let archive = Actors::init::<ksm_runtime::Runtime, _>(
///     client,
///     "ws://127.0.0.1:9944".to_string(),
///     keys.as_slice(),
///     None
/// ).unwrap();
///
/// Actors::block_until_stopped();
///
///
/// ```
pub struct ArchiveContext<T: Substrate + Send + Sync> {
    workers: std::collections::HashMap<String, ChildrenRef>,
    actor_context: ActorContext<T>,
}

impl<T: Substrate + Send + Sync> ArchiveContext<T>
where
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Hash: From<primitive_types::H256>,
    <T as System>::Header: serde::de::DeserializeOwned,
    <T as System>::BlockNumber: From<u32>,
{
    // TODO: Return a reference to the Db pool.
    // just expose a 'shutdown' fn that must be called in order to avoid missing data.
    // or just return an archive object for general telemetry/ops.
    // TODO: Accept one `Config` Struct for which a builder is implemented on
    // to make configuring this easier.
    /// Initialize substrate archive.
    /// Requires a substrate client, url to a running RPC node, and a list of keys to index from storage.
    /// Optionally accepts a URL to the postgreSQL database. However, this can be defined as the
    /// environment variable `DATABASE_URL` instead.
    pub fn init<Runtime, ClientApi>(
        client_api: Arc<ClientApi>,
        backend: Arc<ReadOnlyBackend<NotSignedBlock<T>>>,
        url: String,
        psql_url: &str,
    ) -> Result<Self, ArchiveError>
    where
        Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error>
            + ApiExt<
                NotSignedBlock<T>,
                StateBackend = backend::StateBackendFor<
                    ReadOnlyBackend<NotSignedBlock<T>>,
                    NotSignedBlock<T>,
                >,
            >,
        ClientApi:
            ApiAccess<NotSignedBlock<T>, ReadOnlyBackend<NotSignedBlock<T>>, Runtime> + 'static,
    {
        let mut workers = std::collections::HashMap::new();
        let broker =
            ThreadedBlockExecutor::new(1, Some(8_000_000), client_api.clone(), backend.clone())?;

        Bastion::init();
        let pool = run!(PgPool::builder().max_size(32).build(psql_url))?;

        let context = ActorContext::new(backend.clone(), broker, url, pool.clone());

        // create storage generator here
        // workers.insert("storage".into(), storage.clone());

        // network generator. Gets headers from network but uses client to fetch block bodies
        let blocks = self::generators::blocks::<T>(context.clone())?;
        workers.insert("blocks".into(), blocks);

        let missing_storage = self::generators::missing_storage::<T>(context.clone())?;
        workers.insert("missing_storage".into(), missing_storage);

        // IO/kvdb generator (missing blocks). Queries the database to get missing blocks
        // uses client to get those blocks
        let missing = self::generators::missing_blocks::<T>(context.clone())?;
        workers.insert("missing".into(), missing);

        Bastion::start();

        Ok(Self {
            workers,
            actor_context: context,
        })
    }

    /// Run indefinitely
    /// If the application is shut down during execution, this will leave progress unsaved.
    /// It is recommended to wait for some other event (IE: Ctrl-C) and run `shutdown` instead.
    pub fn block_until_stopped(self) -> Result<(), ArchiveError> {
        Bastion::block_until_stopped();
        self.actor_context.broker.stop()?;
        Ok(())
    }

    /// Shutdown Gracefully.
    /// This makes sure any data we have is saved for the next time substrate-archive is run.
    pub async fn shutdown(self) -> Result<(), ArchiveError> {
        log::info!("Shutting down");
        Bastion::broadcast(Broadcast::Shutdown).expect("Couldn't send messsage");
        for (name, worker) in self.workers.iter() {
            worker
                .stop()
                .expect(format!("Couldn't stop worker {}", name).as_str());
        }
        Bastion::kill();
        self.actor_context.broker.stop()?;
        log::info!("Shut down succesfully");
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArchiveAnswer {
    /// Default answer; will be returned when an 'ask' message is sent to an actor
    Success,
}

/// Messages that are sent to every actor if something happens that must be handled globally
/// like a CTRL-C signal
#[derive(Debug, PartialEq, Clone)]
pub enum Broadcast {
    /// We need to shutdown for one reason or the other
    Shutdown,
}

/// connect to the substrate RPC
/// each actor may potentially have their own RPC connections
async fn connect<T: Substrate + Send + Sync>(url: &str) -> crate::rpc::Rpc<T> {
    crate::rpc::Rpc::connect(url)
        .await
        .expect("Couldn't connect to rpc")
}
