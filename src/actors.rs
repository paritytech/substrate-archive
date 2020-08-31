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
pub mod msg;
mod workers;

pub use self::actor_pool::ActorPool;
use self::workers::GetState;
pub use self::workers::{BlocksIndexer, DatabaseActor, StorageAggregator};
use super::{
    backend::{ApiAccess, Meta, ReadOnlyBackend},
    database::{queries, Channel, Listener},
    error::Result,
    sql_block_builder::BlockBuilder as SqlBlockBuilder,
    tasks::Environment,
    types::Archive,
};
use coil::Job as _;
use futures::FutureExt;
use hashbrown::HashSet;
use sc_client_api::backend;
use serde::de::DeserializeOwned;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use xtra::prelude::*;

// TODO: Split this up into two objects
// System should be a factory that produces objects that should be spawned

/// Context that every actor may use
#[derive(Clone)]
pub struct ActorContext<B: BlockT + Unpin>
where
    B::Hash: Unpin,
{
    backend: Arc<ReadOnlyBackend<B>>,
    pg_url: String,
    meta: Meta<B>,
    workers: usize,
}

impl<B: BlockT + Unpin> ActorContext<B>
where
    B::Hash: Unpin,
{
    pub fn new(
        backend: Arc<ReadOnlyBackend<B>>,
        meta: Meta<B>,
        workers: usize,
        pg_url: String,
    ) -> Self {
        Self {
            backend,
            meta,
            workers,
            pg_url,
        }
    }

    pub fn backend(&self) -> &Arc<ReadOnlyBackend<B>> {
        &self.backend
    }

    pub fn pg_url(&self) -> &str {
        self.pg_url.as_str()
    }
    pub fn meta(&self) -> &Meta<B> {
        &self.meta
    }
}

struct Actors<B: BlockT + Unpin>
where
    B::Hash: Unpin,
    NumberFor<B>: Into<u32>,
{
    storage: Address<workers::StorageAggregator<B>>,
    blocks: Address<workers::BlocksIndexer<B>>,
    metadata: Address<workers::Metadata<B>>,
    db_pool: Address<ActorPool<DatabaseActor<B>>>,
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
    handle: jod_thread::JoinHandle<Result<()>>,
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
        pg_url: &str,
    ) -> Result<Self> {
        let context = ActorContext::new(
            backend.clone(),
            client_api.clone(),
            workers,
            pg_url.to_string(),
        );
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
    pub fn start(
        ctx: ActorContext<B>,
        client: Arc<C>,
    ) -> (
        flume::Sender<()>,
        flume::Sender<()>,
        jod_thread::JoinHandle<Result<()>>,
    ) {
        let (tx_start, rx_start) = flume::bounded(1);
        let (tx_kill, rx_kill) = flume::bounded(1);

        let handle = jod_thread::spawn(move || {
            // block until we receive the message to start
            let _ = rx_start.recv();
            smol::run(Self::main_loop(ctx, rx_kill, client))?;
            Ok(())
        });

        (tx_start, tx_kill, handle)
    }

    async fn main_loop(
        ctx: ActorContext<B>,
        mut rx: flume::Receiver<()>,
        client: Arc<C>,
    ) -> Result<()> {
        let actors = Self::spawn_actors(ctx.clone()).await?;
        let pool = actors
            .db_pool
            .send(GetState::Pool.into())
            .await?
            .await?
            .pool();
        let listener = Self::init_listeners(ctx.pg_url()).await?;
        let mut conn = pool.acquire().await?;
        Self::restore_missing_storage(&mut *conn).await?;
        let env =
            Environment::<B, R, C>::new(ctx.backend().clone(), client, actors.storage.clone());
        let env = AssertUnwindSafe(env);

        let runner = coil::Runner::builder(env, crate::TaskExecutor, &pool)
            .register_job::<crate::tasks::execute_block::Job<B, R, C>>()
            .num_threads(ctx.workers)
            .max_tasks(500)
            .build()?;

        loop {
            let tasks = runner.run_all_sync_tasks().fuse();
            futures::pin_mut!(tasks);
            futures::select! {
                t = tasks => {
                    if t? == 0 {
                        smol::Timer::new(std::time::Duration::from_millis(3600)).await;
                    }
                },
                _ = rx.recv_async() => break,
            }
        }
        listener.kill_async().await;
        Self::kill_actors(actors).await?;
        Ok(())
    }

    async fn spawn_actors(ctx: ActorContext<B>) -> Result<Actors<B>> {
        let db = workers::DatabaseActor::<B>::new(ctx.pg_url().into()).await?;
        let db_pool = actor_pool::ActorPool::new(db, 8).spawn();
        let storage = workers::StorageAggregator::new(db_pool.clone()).spawn();
        let metadata = workers::Metadata::new(db_pool.clone(), ctx.meta().clone())
            .await?
            .spawn();
        let blocks =
            workers::BlocksIndexer::new(ctx.backend().clone(), db_pool.clone(), metadata.clone())
                .spawn();
        Ok(Actors {
            storage,
            blocks,
            metadata,
            db_pool,
        })
    }

    async fn kill_actors(actors: Actors<B>) -> Result<()> {
        let fut = vec![
            actors.storage.send(msg::Die),
            actors.blocks.send(msg::Die),
            actors.metadata.send(msg::Die),
        ];
        futures::future::join_all(fut).await;
        let _ = actors.db_pool.send(msg::Die.into()).await?.await;
        Ok(())
    }

    async fn init_listeners(pg_url: &str) -> Result<Listener> {
        Listener::builder(pg_url, move |notif, conn| {
            async move {
                let block = queries::get_full_block_by_id(conn, notif.id).await?;
                let b: (B, u32) = SqlBlockBuilder::with_single(block)?;
                crate::tasks::execute_block::<B, R, C>(b.0, PhantomData)
                    .enqueue(conn)
                    .await?;
                Ok(())
            }
            .boxed()
        })
        .listen_on(Channel::Blocks)
        .spawn()
        .await
    }

    /// Checks if any blocks that should be executed are missing
    /// from the task queue.
    /// If any are found, they are re-queued.
    async fn restore_missing_storage(conn: &mut sqlx::PgConnection) -> Result<()> {
        log::info!("Restoring missing storage entries...");
        let blocks: HashSet<u32> = queries::get_all_blocks::<B>(conn)
            .await?
            .map(|b| Ok((*b?.header().number()).into()))
            .collect::<Result<_>>()?;
        let mut missing_storage_blocks = queries::blocks_storage_intersection(conn).await?;
        let missing_storage_nums: HashSet<u32> = missing_storage_blocks
            .iter()
            .map(|b| b.block_num as u32)
            .collect();
        let difference: HashSet<u32> = missing_storage_nums
            .difference(&blocks)
            .map(|b| *b)
            .collect();
        missing_storage_blocks.retain(|b| difference.contains(&(b.block_num as u32)));
        let jobs: Vec<crate::tasks::execute_block::Job<B, R, C>> =
            SqlBlockBuilder::with_vec(missing_storage_blocks)?
                .into_iter()
                .map(|b| crate::tasks::execute_block::<B, R, C>(b.inner.block, PhantomData))
                .collect();
        log::info!("Restoring {} missing storage entries", jobs.len());
        coil::JobExt::enqueue_batch(jobs, &mut *conn).await?;
        log::info!("Storage restored");
        Ok(())
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
    fn drive(&mut self) -> Result<()> {
        System::drive(self);
        Ok(())
    }

    async fn block_until_stopped(&self) {
        loop {
            smol::Timer::new(std::time::Duration::from_secs(1)).await;
        }
    }

    fn shutdown(self) -> Result<()> {
        let _ = self.kill_tx.send(());
        self.handle.join()?;
        if let Some(c) = self.context.backend().backing_db().catch_up_count() {
            log::info!("Caught Up {} times", c);
        }
        Ok(())
    }

    fn boxed_shutdown(self: Box<Self>) -> Result<()> {
        let _ = self.kill_tx.send(());
        self.handle.join()?;
        if let Some(c) = self.context.backend().backing_db().catch_up_count() {
            log::info!("Caught Up {} times", c);
        }
        Ok(())
    }

    fn context(&self) -> Result<super::actors::ActorContext<B>> {
        Ok(self.context.clone())
    }
}
