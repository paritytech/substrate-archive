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

mod actor_pool;
mod blocks;
mod generators;
mod workers;

pub use self::actor_pool::ActorPool;
pub use self::workers::{msg, DatabaseActor};

use super::{
    backend::{ApiAccess, BlockChanges, Meta, ReadOnlyBackend},
    database::Listener,
    error::Result,
    threadpools::BlockExecPool,
    types::{Archive, Block, ThreadPool},
};
use futures::FutureExt;
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::marker::PhantomData;
use std::sync::Arc;
use xtra::prelude::*;

struct Die;
impl Message for Die {
    type Result = Result<()>;
}

/// Context that every actor may use
#[derive(Clone)]
pub struct ActorContext<B: BlockT + Unpin>
where
    B::Hash: Unpin,
{
    backend: Arc<ReadOnlyBackend<B>>,
    rpc_url: String,
    psql_url: String,
}

impl<B: BlockT + Unpin> ActorContext<B>
where
    B::Hash: Unpin,
{
    pub fn new(backend: Arc<ReadOnlyBackend<B>>, rpc_url: String, psql_url: String) -> Self {
        Self {
            backend,
            rpc_url,
            psql_url,
        }
    }

    pub fn backend(&self) -> &Arc<ReadOnlyBackend<B>> {
        &self.backend
    }

    pub fn rpc_url(&self) -> &str {
        self.rpc_url.as_str()
    }

    pub fn psql_url(&self) -> &str {
        self.psql_url.as_str()
    }
}

pub struct System<B, R, C>
where
    B: BlockT + Unpin,
    B::Hash: Unpin,
    NumberFor<B>: Into<u32>,
{
    context: ActorContext<B>,
    executor: Arc<dyn ThreadPool<In = Block<B>, Out = BlockChanges<B>>>,
    meta: Meta<B>,
    // ag: Option<Address<Aggregator<B>>>,
    blocks: Option<Address<blocks::BlocksIndexer<B>>>,
    listener: Option<Listener>,
    _marker: PhantomData<(R, C)>,
}

impl<B, R, C> System<B, R, C>
where
    B: BlockT + Unpin,
    R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + sp_api::Metadata<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<ReadOnlyBackend<B>, B>>
        + Send
        + Sync
        + 'static,
    C: ApiAccess<B, ReadOnlyBackend<B>, R> + 'static,
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
        client_api: Arc<C>,
        backend: Arc<ReadOnlyBackend<B>>,
        workers: Option<usize>,
        url: String,
        psql_url: &str,
    ) -> Result<Self> {
        let context = ActorContext::new(backend.clone(), url, psql_url.to_string());
        let executor = Arc::new(BlockExecPool::new(
            workers,
            client_api.clone(),
            backend.clone(),
        )?);

        Ok(Self {
            context,
            executor,
            meta: client_api,
            // ag: None,
            blocks: None,
            listener: None,
            _marker: PhantomData,
        })
    }

    /// Start the actors and begin driving tself.pg_poolheir execution
    pub async fn drive(&mut self) -> Result<()> {
        let ctx = self.context.clone();

        let db = workers::DatabaseActor::<B>::new(ctx.psql_url().into()).await?;
        let db_pool = actor_pool::ActorPool::new(db, 4).spawn();
        let listener = Listener::builder(&ctx.psql_url)
            .listen_on(crate::database::Channel::Blocks)
            .add_task(|_| {
                async move {
                    println!("this is a task!");
                    Ok(())
                }.boxed()
            })
            .on_disconnect(|| {
                async move {
                    println!("Postgres disconnected");
                    Ok(())
                }.boxed()
            })
            .spawn().await?;
        
        // let tx_block = self.executor.sender();
        let meta_addr = workers::Metadata::new(db_pool.clone(), self.meta.clone())
            .await?
            .spawn();

        // super::Generator::new(db_pool.clone(), tx_block.clone()).start()?;

        let blocks_indexer =
            blocks::BlocksIndexer::new(ctx.backend().clone(), db_pool.clone(), meta_addr.clone()).spawn();

        // let exec_stream = self.executor.get_stream();
        // ag.clone().attach_stream(exec_stream);
        self.blocks = Some(blocks_indexer);
        self.listener = Some(listener);

        Ok(())
    }

    pub async fn block_until_stopped(&self) {
        loop {
            smol::Timer::new(std::time::Duration::from_millis(1000)).await;
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<B, R, C> Archive<B> for System<B, R, C>
where
    B: BlockT + Unpin,
    B::Hash: Unpin,
    R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + sp_api::Metadata<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<ReadOnlyBackend<B>, B>>
        + Send
        + Sync
        + 'static,
    C: ApiAccess<B, ReadOnlyBackend<B>, R> + 'static,
    NumberFor<B>: Into<u32> + From<u32> + Unpin,
    B::Hash: From<primitive_types::H256> + Unpin,
    B::Header: serde::de::DeserializeOwned,
{
    async fn drive(&mut self) -> Result<()> {
        System::drive(self).await
    }

    async fn block_until_stopped(&self) {
        System::block_until_stopped(self).await
    }

    fn shutdown(mut self) -> Result<()> {
        if let Some(c) = self.context.backend().backing_db().catch_up_count() {
            log::info!("Caught Up {} times", c);
        }
        /*
        let ag = self.ag.take();
        if let Some(ag) = ag {
            ag.do_send(Die)?;
        }
        */
        Ok(())
    }

    fn boxed_shutdown(mut self: Box<Self>) -> Result<()> {
        if let Some(c) = self.context.backend().backing_db().catch_up_count() {
            log::info!("Caught Up {} times", c);
        }
        /*
        let ag = self.ag.take();
        if let Some(ag) = ag {
            ag.do_send(Die)?;
        }*/
        Ok(())
    }

    fn context(&self) -> Result<super::actors::ActorContext<B>> {
        Ok(self.context.clone())
    }
}
