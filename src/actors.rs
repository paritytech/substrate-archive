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
// mod generators;
mod workers;
pub mod msg;

pub use self::actor_pool::ActorPool;
pub use self::workers::{
    DatabaseActor, StorageAggregator, BlocksIndexer
};
use std::panic::AssertUnwindSafe;
use super::{
    backend::{ApiAccess, Meta, ReadOnlyBackend},
    database::{Listener, Channel, queries},
    error::Result,
    types::{Archive, ActorHandle},
    tasks::Environment,
    sql_block_builder::BlockBuilder as SqlBlockBuilder,
};
use self::workers::GetState;
use futures::FutureExt;
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::marker::PhantomData;
use std::sync::Arc;
use xtra::prelude::*;
use serde::de::DeserializeOwned;
use coil::Job as _;

// TODO: Split this up into two objects
// System should be a factory that produces objects that should be spawned

/// Context that every actor may use
#[derive(Clone)]
pub struct ActorContext<B: BlockT + Unpin>
where
    B::Hash: Unpin,
{
    backend: Arc<ReadOnlyBackend<B>>,
    rpc_url: String,
    pg_url: String,
    meta: Meta<B>,
    workers: usize,
}

impl<B: BlockT + Unpin> ActorContext<B>
where
    B::Hash: Unpin,
{
    pub fn new(backend: Arc<ReadOnlyBackend<B>>, meta: Meta<B>, workers: usize, rpc_url: String, pg_url: String) -> Self {
        Self {
            backend,
            meta,
            workers,
            rpc_url,
            pg_url,
        }
    }

    pub fn backend(&self) -> &Arc<ReadOnlyBackend<B>> {
        &self.backend
    }

    pub fn rpc_url(&self) -> &str {
        self.rpc_url.as_str()
    }

    pub fn pg_url(&self) -> &str {
        self.pg_url.as_str()
    }
    pub fn meta(&self) -> &Meta<B> {
        &self.meta
    }
}

struct Actors<B: BlockT + Unpin> where B::Hash: Unpin, NumberFor<B>: Into<u32> {
    storage: Address<workers::StorageAggregator<B>>,
    blocks: Address<workers::BlocksIndexer<B>>,
    metadata: Address<workers::Metadata<B>>,
    db_pool: Address<ActorPool<DatabaseActor<B>>>
}

/// Control the execution of the indexing engine.
/// Will exit on Drop.
pub struct System<B, R, C>
where
    B: BlockT + Unpin,
    B::Hash: Unpin,
    NumberFor<B>: Into<u32>,
{
    start_tx: flume::Sender<()>,
    kill_tx: flume::Sender<()>,
    context: ActorContext<B>,
    /// handle to the futures runtime indexing the running chain
    handle: jod_thread::JoinHandle<()>,
    _marker: PhantomData<(B, R, C)>,
}

impl<B, R, C> System<B, R, C>
where
    B: BlockT + Unpin + DeserializeOwned,
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
        workers: usize,
        url: String,
        pg_url: &str,
    ) -> Result<Self> {
        let context = ActorContext::new(backend.clone(), client_api.clone(), workers, url, pg_url.to_string());
        let (start_tx, kill_tx, handle) = Self::start(context.clone(), client_api);
        
        Ok(Self {
            context,
            start_tx,
            kill_tx,
            handle,
            _marker: PhantomData,
        })
    }

    fn drive(&self) {
        self.start_tx.send(()).expect("Could not start actors");
    }

    /// Start the actors and begin driving tself.pg_poolheir execution
    pub fn start(ctx: ActorContext<B>, client: Arc<C>) -> (flume::Sender<()>, flume::Sender<()>, jod_thread::JoinHandle<()>) {
        let (tx_start, rx_start) = flume::bounded(1);
        let (tx_kill, rx_kill) = flume::bounded(1);
        
        let handle = jod_thread::spawn(move || {
            // block until we receive the message to start
            let _ = rx_start.recv();
            smol::run(Self::main_loop(ctx, rx_kill, client)).unwrap();
        });
    
        (tx_start, tx_kill, handle)
    }

    async fn main_loop(ctx: ActorContext<B>, mut rx: flume::Receiver<()>, client: Arc<C>) -> Result<()> {
        let actors = Self::spawn_actors(ctx.clone()).await?;
        let pool = actors.db_pool.send(GetState::Pool.into()).await?.await?.pool();
        let listener = Self::init_listeners(ctx.pg_url()).await?;

        let env = Environment::<B, R, C>::new(ctx.backend().clone(), client, actors.storage.clone());
        let env = AssertUnwindSafe(env); 
        
        let runner = coil::Runner::builder(env, crate::TaskExecutor, &pool)
            .register_job::<crate::tasks::execute_block::Job<B, R, C>>()
            .num_threads(ctx.workers)
            .max_tasks(500)
            .build()?;
        
        loop {
            let tasks = runner.run_all_sync_tasks().fuse();
            futures::pin_mut!(tasks);
            futures::select!{
                t = tasks => {
                    if t? == 0 {
                        smol::Timer::new(std::time::Duration::from_millis(3600)).await;
                    }
                },
                _ = rx.recv_async() => break,
            }
        }
        listener.kill_async().await;
        Ok(())
    }

    async fn spawn_actors(ctx: ActorContext<B>) -> Result<Actors<B>> {
        let db = workers::DatabaseActor::<B>::new(ctx.pg_url().into()).await?;
        let db_pool = actor_pool::ActorPool::new(db, 4)
            .spawn();
        let storage = workers::StorageAggregator::new(db_pool.clone()).spawn();
        let metadata = workers::Metadata::new(db_pool.clone(), ctx.meta().clone())
            .await?
            .spawn();
        let blocks = workers::BlocksIndexer::new(ctx.backend().clone(), db_pool.clone(), metadata.clone())
            .spawn();
        Ok(Actors {
            storage, blocks, metadata, db_pool
        })
    }
    
    async fn init_listeners(pg_url: &str) -> Result<Listener> {
        Listener::builder(pg_url, move |notif, conn| async move {
                    let block = queries::get_full_block_by_id(conn, notif.id).await?;
                    let b: (B, u32) = SqlBlockBuilder::with_single(block)?;
                    crate::tasks::execute_block::<B, R, C>(b.0, PhantomData).enqueue(conn).await?;
                    Ok(())
                }.boxed())
            .listen_on(Channel::Blocks)
            .on_disconnect(|| {
                panic!("PostgreSQL Disconnected! TODO");
            }).spawn().await
    }
}

#[async_trait::async_trait(?Send)]
impl<B, R, C> Archive<B> for System<B, R, C>
where
    B: BlockT + Unpin + DeserializeOwned,
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
        System::drive(self);
        Ok(())
    }

    async fn block_until_stopped(&self) {
        loop {
            smol::Timer::new(std::time::Duration::from_secs(1)).await;
        }
    }

    fn shutdown(mut self) -> Result<()> {
        let _ = self.kill_tx.send(());
        if let Some(c) = self.context.backend().backing_db().catch_up_count() {
            log::info!("Caught Up {} times", c);
        }
        Ok(())
    }

    fn boxed_shutdown(mut self: Box<Self>) -> Result<()> {
        let _ = self.kill_tx.send(());
        if let Some(c) = self.context.backend().backing_db().catch_up_count() {
            log::info!("Caught Up {} times", c);
        }
        Ok(())
    }

    fn context(&self) -> Result<super::actors::ActorContext<B>> {
        Ok(self.context.clone())
    }
}
