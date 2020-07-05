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

use self::generators::{fill_storage, missing_blocks};

use super::{
    backend::{ApiAccess, GetRuntimeVersion, ReadOnlyBackend},
    error::{ArchiveResult, Error as ArchiveError},
    threadpools::{BlockFetcher, ThreadedBlockExecutor},
    types::Archive,
};
use futures::{
    stream::{FuturesUnordered, StreamExt},
    Future,
};
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use sqlx::postgres::PgPool;
use std::marker::PhantomData;
use std::sync::Arc;
use workers::msg;
pub use workers::Aggregator;
use xtra::prelude::*;

/// Context that every actor may use
#[derive(Clone)]
pub struct ActorContext<Block: BlockT> {
    backend: Arc<ReadOnlyBackend<Block>>,
    api: Arc<dyn GetRuntimeVersion<Block>>,
    rpc_url: String,
    psql_url: String,
}

impl<Block: BlockT> ActorContext<Block> {
    pub fn new(
        backend: Arc<ReadOnlyBackend<Block>>,
        rpc_url: String,
        psql_url: String,
        api: Arc<dyn GetRuntimeVersion<Block>>,
    ) -> Self {
        Self {
            backend,
            rpc_url,
            psql_url,
            api,
        }
    }

    pub fn backend(&self) -> &Arc<ReadOnlyBackend<Block>> {
        &self.backend
    }

    pub fn rpc_url(&self) -> &str {
        self.rpc_url.as_str()
    }

    pub fn api(&self) -> Arc<dyn GetRuntimeVersion<Block>> {
        self.api.clone()
    }

    pub fn psql_url(&self) -> &str {
        self.psql_url.as_str()
    }
}

#[derive(Clone)]
pub struct System<Block: BlockT, R, C> {
    context: ActorContext<Block>,
    workers: Option<usize>,
    api: Arc<C>,
    _marker: PhantomData<(R, C)>,
}

impl<B, R, C> System<B, R, C>
where
    B: BlockT,
    R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<ReadOnlyBackend<B>, B>>
        + Send
        + Sync
        + 'static,
    C: ApiAccess<B, ReadOnlyBackend<B>, R> + GetRuntimeVersion<B> + 'static,
    NumberFor<B>: Into<u32> + From<u32> + Unpin,
    B::Hash: From<primitive_types::H256> + Unpin,
    B::Header: serde::de::DeserializeOwned,
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
    pub fn new(
        // one client per-threadpool. This way we don't have conflicting cache resources
        // for WASM runtime-instances
        client_api: (Arc<C>, Arc<C>),
        backend: Arc<ReadOnlyBackend<B>>,
        workers: Option<usize>,
        url: String,
        psql_url: &str,
    ) -> ArchiveResult<Self> {
        let (api, blk_client) = client_api;
        let context = ActorContext::new(backend.clone(), url, psql_url.to_string(), blk_client);

        Ok(Self {
            context,
            workers,
            api,
            _marker: PhantomData,
        })
    }

    /// Start the actors and begin driving their execution
    // FIXME: don't make drive mut self
    pub async fn drive(&mut self) -> ArchiveResult<()> {
        let pool = PgPool::builder()
            .max_size(16)
            .build(self.context.psql_url())
            .await?;
        let backend = self.context.backend.clone();

        let exec_pool =
            ThreadedBlockExecutor::new(self.api.clone(), backend, self.workers.clone())?;
        let tx = exec_pool.sender();
        let ctx = self.context.clone();

        let fetch_pool = BlockFetcher::new(ctx.clone(), Some(3))?;

        let rpc = crate::rpc::Rpc::<B>::connect(ctx.rpc_url()).await?;
        let subscription = rpc.subscribe_finalized_heads().await?;
        let ag = Aggregator::new(ctx.rpc_url(), tx, &pool).spawn();

        // fetch.attach_stream(subscription.map(|h| msg::Head(h)));
        // ag.attach_stream(results);
        Ok(())
    }

    pub async fn block_until_stopped(&self) -> () {
        loop {
            timer::Delay::new(std::time::Duration::from_secs(1)).await;
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<B: BlockT, R, C> Archive<B> for System<B, R, C> {
    async fn drive(&mut self) -> Result<(), ArchiveError> {
        self.drive().await
    }

    async fn block_until_stopped(&self) -> () {
        self.block_until_stopped().await
    }

    fn shutdown(self) -> Result<(), ArchiveError> {
        self.shutdown()?;
        Ok(())
    }

    fn context(&self) -> Result<super::actors::ActorContext<B>, ArchiveError> {
        Ok(self.context.clone())
    }
}

/*
fn start_generators<B: BlockT>(pool: sqlx::PgPool) {
    crate::util::spawn(missing_blocks(pool, fetch.clone()));
    crate::util::spawn(fill_storage(pool, broker));
}
*/

/// connect to the substrate RPC
/// each actor may potentially have their own RPC connections
async fn connect<Block: BlockT>(url: &str) -> crate::rpc::Rpc<Block> {
    crate::rpc::Rpc::connect(url)
        .await
        .expect("Couldn't connect to rpc")
}
