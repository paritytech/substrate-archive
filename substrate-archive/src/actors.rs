// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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
mod workers;

use std::{marker::PhantomData, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use coil::Job as _;
use futures::{future::BoxFuture, FutureExt};
use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};
use xtra::{prelude::*, spawn::Smol, Disconnected};

use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_blockchain::Error as BlockchainError;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};

use substrate_archive_backend::{ApiAccess, Meta, ReadOnlyBackend, ReadOnlyDB};

use self::workers::GetState;
pub use self::{
	actor_pool::ActorPool,
	workers::{BlocksIndexer, DatabaseActor, StorageAggregator},
};
use crate::{
	archive::Archive,
	database::{models::BlockModelDecoder, queries, Channel, Listener},
	error::Result,
	tasks::{Environment, TaskExecutor},
	types::Die,
};

// TODO: Split this up into two objects
// System should be a factory that produces objects that should be spawned

/// Provides parameters that are passed in from the user.
/// Provides context that every actor may use
pub struct SystemConfig<B: BlockT + Unpin, D: ReadOnlyDB + 'static>
where
	B::Hash: Unpin,
{
	pub backend: Arc<ReadOnlyBackend<B, D>>,
	pub pg_url: String,
	pub meta: Meta<B>,
	pub control: ControlConfig,
	pub tracing_targets: Option<String>,
}

impl<B: BlockT + Unpin, D: ReadOnlyDB> Clone for SystemConfig<B, D>
where
	B::Hash: Unpin,
{
	fn clone(&self) -> SystemConfig<B, D> {
		SystemConfig {
			backend: Arc::clone(&self.backend),
			pg_url: self.pg_url.clone(),
			meta: self.meta.clone(),
			control: self.control,
			tracing_targets: self.tracing_targets.clone(),
		}
	}
}

#[derive(Copy, Clone, Debug, Deserialize)]
pub struct ControlConfig {
	/// Number of database actors to be spawned in the actor pool.
	#[serde(default = "default_db_actor_pool_size")]
	pub(crate) db_actor_pool_size: usize,
	/// Number of threads to spawn for task execution.
	#[serde(default = "default_task_workers")]
	pub(crate) task_workers: usize,
	/// Maximum amount of time coil will wait for a task to begin.
	/// Times out if tasks don't start execution in the threadpool within `task_timeout` seconds.
	#[serde(default = "default_task_timeout")]
	pub(crate) task_timeout: u64,
	/// Maximum tasks to queue in the threadpool.
	#[serde(default = "default_task_workers")]
	pub(crate) max_tasks: usize,
	/// Maximum amount of blocks to index at once.
	#[serde(default = "default_max_block_load")]
	pub(crate) max_block_load: u32,
}

impl Default for ControlConfig {
	fn default() -> Self {
		Self {
			db_actor_pool_size: default_db_actor_pool_size(),
			task_workers: default_task_workers(),
			task_timeout: default_task_timeout(),
			max_tasks: default_max_tasks(),
			max_block_load: default_max_block_load(),
		}
	}
}

const fn default_db_actor_pool_size() -> usize {
	4
}

fn default_task_workers() -> usize {
	num_cpus::get()
}

const fn default_task_timeout() -> u64 {
	20
}

const fn default_max_tasks() -> usize {
	64
}

const fn default_max_block_load() -> u32 {
	100_000
}

impl<B: BlockT + Unpin, D: ReadOnlyDB> SystemConfig<B, D>
where
	B::Hash: Unpin,
{
	pub fn new(
		backend: Arc<ReadOnlyBackend<B, D>>,
		pg_url: String,
		meta: Meta<B>,
		control: ControlConfig,
		tracing_targets: Option<String>,
	) -> Self {
		Self { backend, pg_url, meta, control, tracing_targets }
	}

	pub fn backend(&self) -> &Arc<ReadOnlyBackend<B, D>> {
		&self.backend
	}

	pub fn pg_url(&self) -> &str {
		self.pg_url.as_str()
	}

	pub fn meta(&self) -> &Meta<B> {
		&self.meta
	}
}

struct Actors<B: BlockT + Unpin, D: ReadOnlyDB + 'static>
where
	B::Hash: Unpin,
	NumberFor<B>: Into<u32>,
{
	storage: Address<workers::StorageAggregator<B>>,
	blocks: Address<workers::BlocksIndexer<B, D>>,
	metadata: Address<workers::MetadataActor<B>>,
	db_pool: Address<ActorPool<DatabaseActor<B>>>,
}

/// Control the execution of the indexing engine.
/// Will exit on Drop.
pub struct System<B, R, C, D>
where
	D: ReadOnlyDB + 'static,
	B: BlockT + Unpin,
	B::Hash: Unpin,
	NumberFor<B>: Into<u32>,
{
	config: SystemConfig<B, D>,
	start_tx: flume::Sender<()>,
	kill_tx: flume::Sender<()>,
	/// handle to the futures runtime indexing the running chain
	handle: jod_thread::JoinHandle<Result<()>>,
	_marker: PhantomData<(B, R, C, D)>,
}

impl<B, R, C, D> System<B, R, C, D>
where
	D: ReadOnlyDB + 'static,
	B: BlockT + Unpin + DeserializeOwned,
	R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
	R::RuntimeApi: BlockBuilderApi<B, Error = BlockchainError>
		+ sp_api::Metadata<B, Error = BlockchainError>
		+ ApiExt<B, StateBackend = backend::StateBackendFor<ReadOnlyBackend<B, D>, B>>
		+ Send
		+ Sync
		+ 'static,
	C: ApiAccess<B, ReadOnlyBackend<B, D>, R> + 'static,
	NumberFor<B>: Into<u32> + From<u32> + Unpin,
	B::Hash: Unpin,
	B::Header: serde::de::DeserializeOwned,
{
	/// Initialize substrate archive.
	/// Requires a substrate client, url to a running RPC node, and a list of keys to index from storage.
	/// Optionally accepts a URL to the postgreSQL database. However, this can be defined as the
	/// environment variable `DATABASE_URL` instead.
	pub fn new(
		// one client per-threadpool. This way we don't have conflicting cache resources
		// for WASM runtime-instances
		client_api: Arc<C>,
		config: SystemConfig<B, D>,
	) -> Result<Self> {
		let (start_tx, kill_tx, handle) = Self::start(config.clone(), client_api);

		Ok(Self { config, start_tx, kill_tx, handle, _marker: PhantomData })
	}

	fn drive(&self) {
		self.start_tx.send(()).expect("Could not start actors");
	}

	/// Start the actors and begin driving their execution
	pub fn start(
		conf: SystemConfig<B, D>,
		client: Arc<C>,
	) -> (flume::Sender<()>, flume::Sender<()>, jod_thread::JoinHandle<Result<()>>) {
		let (tx_start, rx_start) = flume::bounded(1);
		let (tx_kill, rx_kill) = flume::bounded(1);

		let handle = jod_thread::spawn(move || {
			// block until we receive the message to start
			let _ = rx_start.recv();

			smol::block_on(Self::main_loop(conf, rx_kill, client))?;
			Ok(())
		});

		(tx_start, tx_kill, handle)
	}

	async fn main_loop(conf: SystemConfig<B, D>, rx: flume::Receiver<()>, client: Arc<C>) -> Result<()> {
		let actors = Self::spawn_actors(conf.clone()).await?;
		let pool = actors.db_pool.send(GetState::Pool.into()).await?.await?.pool();
		let listener = Self::init_listeners(conf.pg_url()).await?;
		let mut conn = pool.acquire().await?;
		Self::restore_missing_storage(&mut *conn).await?;
		let env = Environment::<B, R, C, D>::new(
			conf.backend().clone(),
			client,
			actors.storage.clone(),
			conf.tracing_targets.clone(),
		);
		let env = AssertUnwindSafe(env);

		let runner = coil::Runner::builder(env, TaskExecutor, &pool)
			.register_job::<crate::tasks::execute_block::Job<B, R, C, D>>()
			.num_threads(conf.control.task_workers)
			// times out if tasks don't start execution on the threadpool within 20 seconds.
			.timeout(Duration::from_secs(conf.control.task_timeout))
			.max_tasks(conf.control.max_tasks)
			.build()?;

		loop {
			let tasks = runner.run_all_sync_tasks().fuse();
			futures::pin_mut!(tasks);
			futures::select! {
				t = tasks => {
					match t {
						Ok(0) => {
							smol::Timer::after(std::time::Duration::from_millis(500)).await;
						},
						Ok(n) => log::debug!("Executed {} tasks successfully", n),
						Err(coil::FetchError::Timeout) => log::warn!("Tasks timed out"),
						Err(e) => log::error!("{:?}", e),
					}
				},
				_ = rx.recv_async() => break,
			}
		}
		Self::kill_actors(actors).await?;
		listener.kill_async().await;
		Ok(())
	}

	async fn spawn_actors(conf: SystemConfig<B, D>) -> Result<Actors<B, D>> {
		let db = workers::DatabaseActor::<B>::new(conf.pg_url().into()).await?;
		let db_pool =
			actor_pool::ActorPool::new(db, conf.control.db_actor_pool_size).create(None).spawn(&mut Smol::Global);
		let storage = workers::StorageAggregator::new(db_pool.clone()).create(None).spawn(&mut Smol::Global);
		let metadata = workers::MetadataActor::new(db_pool.clone(), conf.meta().clone())
			.await?
			.create(None)
			.spawn(&mut Smol::Global);
		let blocks =
			workers::BlocksIndexer::new(&conf, db_pool.clone(), metadata.clone()).create(None).spawn(&mut Smol::Global);

		Ok(Actors { storage, blocks, metadata, db_pool })
	}

	async fn kill_actors(actors: Actors<B, D>) -> Result<()> {
		let fut: Vec<BoxFuture<'_, Result<(), Disconnected>>> = vec![
			Box::pin(actors.storage.send(Die)),
			Box::pin(actors.blocks.send(Die)),
			Box::pin(actors.metadata.send(Die)),
		];
		futures::future::join_all(fut).await;
		let _ = actors.db_pool.send(Die.into()).await?.await;
		Ok(())
	}

	async fn init_listeners(pg_url: &str) -> Result<Listener> {
		Listener::builder(pg_url, move |notif, conn| {
			async move {
				let block = queries::get_full_block_by_id(conn, notif.id).await?;
				let b: (B, u32) = BlockModelDecoder::with_single(block)?;
				crate::tasks::execute_block::<B, R, C, D>(b.0, PhantomData).enqueue(conn).await?;
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
		let blocks: HashSet<u32> = queries::get_all_blocks::<B>(conn)
			.await?
			.map(|b| Ok((*b?.header().number()).into()))
			.collect::<Result<_>>()?;
		let mut missing_storage_blocks = queries::blocks_storage_intersection(conn).await?;
		let difference: HashSet<u32> = missing_storage_blocks
			.iter()
			.map(|b| b.block_num as u32)
			.collect::<HashSet<u32>>()
			.difference(&blocks)
			.copied()
			.collect();
		missing_storage_blocks.retain(|b| difference.contains(&(b.block_num as u32)));
		let jobs: Vec<crate::tasks::execute_block::Job<B, R, C, D>> =
			BlockModelDecoder::with_vec(missing_storage_blocks)?
				.into_iter()
				.map(|b| crate::tasks::execute_block::<B, R, C, D>(b.inner.block, PhantomData))
				.collect();
		log::info!("Restoring {} missing storage entries. This could take a few minutes...", jobs.len());
		coil::JobExt::enqueue_batch(jobs, &mut *conn).await?;
		log::info!("Storage restored");
		Ok(())
	}
}

#[async_trait::async_trait(?Send)]
impl<B, R, C, D> Archive<B, D> for System<B, R, C, D>
where
	D: ReadOnlyDB + 'static,
	B: BlockT + Unpin + DeserializeOwned,
	<B as BlockT>::Hash: Unpin,
	R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
	R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
		+ sp_api::Metadata<B, Error = sp_blockchain::Error>
		+ ApiExt<B, StateBackend = backend::StateBackendFor<ReadOnlyBackend<B, D>, B>>
		+ Send
		+ Sync
		+ 'static,
	C: ApiAccess<B, ReadOnlyBackend<B, D>, R> + 'static,
	NumberFor<B>: Into<u32> + From<u32> + Unpin,
	B::Hash: Unpin,
	B::Header: serde::de::DeserializeOwned,
{
	fn drive(&mut self) -> Result<()> {
		System::drive(self);
		Ok(())
	}

	async fn block_until_stopped(&self) {
		loop {
			smol::Timer::after(std::time::Duration::from_secs(1)).await;
		}
	}

	fn shutdown(self) -> Result<()> {
		let _ = self.kill_tx.send(());
		self.handle.join()?;
		Ok(())
	}

	fn boxed_shutdown(self: Box<Self>) -> Result<()> {
		let _ = self.kill_tx.send(());
		self.handle.join()?;
		Ok(())
	}

	fn context(&self) -> &SystemConfig<B, D> {
		&self.config
	}
}
