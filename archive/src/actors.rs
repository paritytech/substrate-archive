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
mod workers;

use super::{
    backend::{ApiAccess, BlockBroker, GetRuntimeVersion, ReadOnlyBackend, ThreadedBlockExecutor},
    error::{ArchiveResult, Error as ArchiveError},
};
use generators::{BlocksActor, MissingStorage};
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use sqlx::postgres::PgPool;
use std::sync::Arc;
use xtra::prelude::*;

/// Context that every actor may use
#[derive(Clone)]
pub struct ActorContext<Block: BlockT> {
    backend: Arc<ReadOnlyBackend<Block>>,
    broker: BlockBroker<Block>,
    rpc_url: String,
    pool: sqlx::PgPool,
}

impl<Block: BlockT> ActorContext<Block> {
    pub fn new(
        backend: Arc<ReadOnlyBackend<Block>>,
        broker: BlockBroker<Block>,
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

    pub fn backend(&self) -> &Arc<ReadOnlyBackend<Block>> {
        &self.backend
    }

    pub fn rpc_url(&self) -> &str {
        self.rpc_url.as_str()
    }

    pub fn pool(&self) -> sqlx::PgPool {
        self.pool.clone()
    }
    pub fn broker(&self) -> &BlockBroker<Block> {
        &self.broker
    }
}

/// Main entrypoint for substrate-archive.
/// Deals with starting, stopping and manipulating the Archive Runtime
///
/// # Examples
///
/// ```
/// use polkadot_service::{kusama_runtime::RuntimeApi as RApi, Block, KusamaExecutor as KExec};
/// use substrate_archive::{Archive, ArchiveConfig, MigrationConfig};
/// let conf = ArchiveConfig {
///     db_url: "/home/insipx/.local/share/polkadot/chains/ksmcc3/db".into(),
///     rpc_url: "ws://127.0.0.1:9944".into(),
///     cache_size: 1024,
///     block_workers: None,
///     wasm_pages: None,
///     psql_conf: MigrationConfig {
///         host: None,
///         port: None,
///         user: Some("archive".to_string()),
///         pass: Some("default".to_string()),
///         name: Some("kusama-archive".to_string()),
///     },
/// };
///
/// let spec = polkadot_service::chain_spec::kusama_config().unwrap();
/// let archive = Archive::<Block, RApi, KExec>::new(conf, Box::new(spec)).unwrap();
/// let archive = archive.run().unwrap();
///
/// archive.block_until_stopped();
///
/// ```
pub struct ArchiveContext<Block: BlockT> {
    actor_context: ActorContext<Block>,
    rt: tokio::runtime::Runtime,
    blocks: BlocksActor<Block>,
    storage: MissingStorage<Block>,
}

impl<Block> ArchiveContext<Block>
where
    Block: BlockT,
    NumberFor<Block>: Into<u32>,
    NumberFor<Block>: From<u32>,
    Block::Hash: From<primitive_types::H256>,
    Block::Header: serde::de::DeserializeOwned,
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
        backend: Arc<ReadOnlyBackend<Block>>,
        block_workers: Option<usize>,
        url: String,
        psql_url: &str,
    ) -> Result<Self, ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, ClientApi> + Send + Sync + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<Block, Error = sp_blockchain::Error>
            + ApiExt<Block, StateBackend = backend::StateBackendFor<ReadOnlyBackend<Block>, Block>>
            + Send
            + Sync
            + 'static,
        ClientApi: ApiAccess<Block, ReadOnlyBackend<Block>, Runtime> + GetRuntimeVersion<Block> + 'static,
    {
        let broker = ThreadedBlockExecutor::new(block_workers, client_api.clone(), backend.clone())?;
        let mut rt = tokio::runtime::Runtime::new()?;
        let pool = rt.block_on(PgPool::builder().max_size(8).build(psql_url))?;
        let context = ActorContext::new(backend.clone(), broker, url, pool.clone());

        let context0 = context.clone();
        let main = || -> ArchiveResult<(BlocksActor<Block>, MissingStorage<Block>)> {
            let url = context0.rpc_url().to_string();
            let addr = workers::BlockFetcher::new(&context0, client_api.clone(), Some(3))?.spawn();
            let blocks = BlocksActor::new(url, addr.clone());
            let storage = MissingStorage::new(context0.clone())?;
            tokio::spawn(generators::missing_blocks(context0.clone(), addr));
            Ok((blocks, storage))
        };
        let (blocks, storage) = rt.enter(main)?;

        Ok(Self {
            rt,
            blocks,
            storage,
            actor_context: context,
        })
    }

    #[deprecated(since = "0.4.1", note = "use the shutdown method instead")]
    pub fn block_until_stopped(mut self) -> Result<(), ArchiveError> {
        self.rt.block_on(async {
            loop {
                timer::Delay::new(std::time::Duration::from_secs(1)).await;
            }
        });
        Ok(())
    }

    /// Shutdown Gracefully.
    /// This makes sure any data we have is saved for the next time substrate-archive is run.
    pub fn shutdown(self) -> Result<(), ArchiveError> {
        log::info!("Shutting down");
        // self.missing_blocks.join().expect("Could not join");
        self.rt.shutdown_timeout(std::time::Duration::from_secs(5));
        self.actor_context.broker.stop()?;
        log::info!("Shut down succesfully");
        Ok(())
    }
}

/// connect to the substrate RPC
/// each actor may potentially have their own RPC connections
async fn connect<Block: BlockT>(url: &str) -> crate::rpc::Rpc<Block> {
    crate::rpc::Rpc::connect(url)
        .await
        .expect("Couldn't connect to rpc")
}
