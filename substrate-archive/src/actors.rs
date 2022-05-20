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

//! Main entrypoint for substrate-archive. `init` will start the actor loop and begin indexing the
//! chain defined with the passed-in Client and URL.

mod workers;

use std::{
	convert::TryInto,
	marker::PhantomData,
	panic::AssertUnwindSafe,
	sync::Arc,
	time::{Duration, Instant},
};

use async_std::{
	future::timeout,
	task::{self, JoinHandle},
};
use futures::{future, FutureExt, StreamExt};
use futures_timer::Delay;
use sa_work_queue::{Job as _, QueueHandle, Runner};
use serde::{de::DeserializeOwned, Deserialize};
use xtra::{prelude::*, spawn::AsyncStd};

use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};

use substrate_archive_backend::{ApiAccess, Meta, ReadOnlyBackend, ReadOnlyDb, RuntimeConfig};

use self::workers::{
	blocks::{Crawl, ReIndex},
	database::GetState,
	extrinsics_decoder::Index,
	storage_aggregator::{SendStorage, SendTraces},
};
pub use self::workers::{BlocksIndexer, DatabaseActor, ExtrinsicsDecoder, StorageAggregator};
use crate::{
	archive::Archive,
	database::{
		models::{BlockModelDecoder, PersistentConfig},
		queries, Channel, Listener,
	},
	error::Result,
	tasks::Environment,
};

/// Provides parameters that are passed in from the user.
/// Provides context that every actor may use
pub struct SystemConfig<Block, Db> {
	pub backend: Arc<ReadOnlyBackend<Block, Db>>,
	pub pg_url: String,
	pub meta: Meta<Block>,
	pub control: ControlConfig,
	pub runtime: RuntimeConfig,
	pub tracing_targets: Option<String>,
	persistent_config: PersistentConfig,
}

impl<Block, Db> Clone for SystemConfig<Block, Db> {
	fn clone(&self) -> SystemConfig<Block, Db> {
		SystemConfig {
			backend: Arc::clone(&self.backend),
			pg_url: self.pg_url.clone(),
			meta: self.meta.clone(),
			control: self.control.clone(),
			runtime: self.runtime.clone(),
			tracing_targets: self.tracing_targets.clone(),
			persistent_config: self.persistent_config.clone(),
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct ControlConfig {
	/// Maximum amount of time the work queue will wait for a task to begin.
	/// Times out if tasks don't start execution in the thread pool within `task_timeout` seconds.
	#[serde(default = "default_task_timeout")]
	pub(crate) task_timeout: u64,
	/// Maximum amount of blocks to index at once.
	#[serde(default = "default_max_block_load")]
	pub(crate) max_block_load: u32,
	/// RabbitMq URL. default: `amqp://localhost:5672`
	#[serde(default = "default_task_url")]
	pub(crate) task_url: String,
	/// Whether to index storage or not
	#[serde(default = "default_storage_indexing")]
	pub(crate) storage_indexing: bool,
}

impl Default for ControlConfig {
	fn default() -> Self {
		Self {
			task_timeout: default_task_timeout(),
			max_block_load: default_max_block_load(),
			task_url: default_task_url(),
			storage_indexing: default_storage_indexing(),
		}
	}
}

const fn default_storage_indexing() -> bool {
	true
}

fn default_task_url() -> String {
	"amqp://localhost:5672".into()
}

const fn default_task_timeout() -> u64 {
	20
}

const fn default_max_block_load() -> u32 {
	100_000
}

impl<Block: BlockT + Unpin, Db: ReadOnlyDb> SystemConfig<Block, Db>
where
	Block::Hash: Unpin,
{
	pub fn new(
		backend: Arc<ReadOnlyBackend<Block, Db>>,
		pg_url: String,
		meta: Meta<Block>,
		control: ControlConfig,
		runtime: RuntimeConfig,
		tracing_targets: Option<String>,
		persistent_config: PersistentConfig,
	) -> Self {
		Self { backend, pg_url, meta, control, runtime, tracing_targets, persistent_config }
	}

	pub fn backend(&self) -> &Arc<ReadOnlyBackend<Block, Db>> {
		&self.backend
	}

	pub fn pg_url(&self) -> &str {
		self.pg_url.as_str()
	}

	pub fn meta(&self) -> &Meta<Block> {
		&self.meta
	}
}

struct Actors<Block: Send + Sync + 'static, Hash: Send + Sync + 'static, Db: Send + Sync + 'static> {
	storage: Address<workers::StorageAggregator<Hash>>,
	blocks: Address<workers::BlocksIndexer<Block, Db>>,
	metadata: Address<workers::MetadataActor<Block>>,
	db: Address<DatabaseActor>,
	extrinsics: Address<ExtrinsicsDecoder>,
}

impl<Block: Send + Sync + 'static, Hash: Send + Sync + 'static, Db: Send + Sync + 'static> Clone
	for Actors<Block, Hash, Db>
{
	fn clone(&self) -> Self {
		Self {
			storage: self.storage.clone(),
			blocks: self.blocks.clone(),
			metadata: self.metadata.clone(),
			db: self.db.clone(),
			extrinsics: self.extrinsics.clone(),
		}
	}
}

impl<Block, Db> Actors<Block, Block::Hash, Db>
where
	Block: BlockT + Unpin,
	Db: ReadOnlyDb + 'static,
	Block::Hash: Unpin,
	NumberFor<Block>: Into<u32>,
{
	async fn spawn(conf: &SystemConfig<Block, Db>) -> Result<Self> {
		let db = workers::DatabaseActor::new(conf.pg_url()).await?.create(None).spawn(&mut AsyncStd);
		let storage = workers::StorageAggregator::new(db.clone()).create(None).spawn(&mut AsyncStd);
		let metadata =
			workers::MetadataActor::new(db.clone(), conf.meta().clone()).await?.create(None).spawn(&mut AsyncStd);
		let blocks = workers::BlocksIndexer::new(conf, db.clone(), metadata.clone()).create(None).spawn(&mut AsyncStd);
		let extrinsics = workers::ExtrinsicsDecoder::new(conf, db.clone()).await?.create(None).spawn(&mut AsyncStd);

		Ok(Actors { storage, blocks, metadata, db, extrinsics })
	}

	/// Run a future that sends actors a signal to progress once the previous
	/// messages have been processed.
	async fn tick_interval(&self) -> Result<()> {
		// messages that only need to be sent once
		self.blocks.send(ReIndex).await?;
		let actors = self.clone();
		task::spawn(async move {
			loop {
				let fut = (
					Box::pin(actors.blocks.send(Crawl)),
					Box::pin(actors.storage.send(SendStorage)),
					Box::pin(actors.storage.send(SendTraces)),
					Box::pin(actors.extrinsics.send(Index)),
				);
				if future::try_join4(fut.0, fut.1, fut.2, fut.3).await.is_err() {
					break;
				}
			}
		})
		.await;
		Ok(())
	}
}

/// Control the execution of the indexing engine.
/// Will exit on Drop.
pub struct System<B, R, C, D>
where
	D: ReadOnlyDb + 'static,
	B: BlockT + Unpin,
	B::Hash: Unpin,
	NumberFor<B>: Into<u32>,
{
	config: SystemConfig<B, D>,
	/// handle to the futures runtime indexing the running chain
	handle: Option<JoinHandle<Result<()>>>,
	client: Arc<C>,
	_marker: PhantomData<(B, R, D)>,
}

impl<Block, Runtime, Client, Db> System<Block, Runtime, Client, Db>
where
	Db: ReadOnlyDb + 'static,
	Block: BlockT + Unpin + DeserializeOwned,
	Runtime: ConstructRuntimeApi<Block, Client> + Send + Sync + 'static,
	Runtime::RuntimeApi: BlockBuilderApi<Block>
		+ sp_api::Metadata<Block>
		+ ApiExt<Block, StateBackend = backend::StateBackendFor<ReadOnlyBackend<Block, Db>, Block>>
		+ Send
		+ Sync
		+ 'static,
	Client: ApiAccess<Block, ReadOnlyBackend<Block, Db>, Runtime> + 'static,
	NumberFor<Block>: Into<u32> + From<u32> + Unpin,
	Block::Hash: Unpin,
	Block::Header: serde::de::DeserializeOwned,
{
	/// Initialize substrate archive.
	/// Requires a substrate client, url to a running RPC node, and a list of keys to index from storage.
	/// Optionally accepts a URL to the postgreSQL database. However, this can be defined as the
	/// environment variable `DATABASE_URL` instead.
	pub fn new(
		// one client per-threadpool. This way we don't have conflicting cache resources
		// for WASM runtime-instances
		client: Arc<Client>,
		config: SystemConfig<Block, Db>,
	) -> Result<Self> {
		Ok(Self { handle: None, config, client, _marker: PhantomData })
	}

	fn drive(&mut self) -> Result<()> {
		let instance = SystemInstance::new(self.config.clone(), self.client.clone())?;
		let handle = task::spawn(instance.work());
		self.handle.replace(handle);
		Ok(())
	}
}

type TaskRunner<Block, Hash, Runtime, Client, Db> =
	Runner<AssertUnwindSafe<Environment<Block, Hash, Runtime, Client, Db>>>;

pub struct SystemInstance<Block, Runtime, Db, Client> {
	config: SystemConfig<Block, Db>,
	client: Arc<Client>,
	_marker: PhantomData<Runtime>,
}

impl<Block, Runtime, Db, Client> SystemInstance<Block, Runtime, Db, Client>
where
	Db: ReadOnlyDb + 'static,
	Block: BlockT + Unpin + DeserializeOwned,
	Runtime: ConstructRuntimeApi<Block, Client> + Send + Sync + 'static,
	Runtime::RuntimeApi: BlockBuilderApi<Block>
		+ sp_api::Metadata<Block>
		+ ApiExt<Block, StateBackend = backend::StateBackendFor<ReadOnlyBackend<Block, Db>, Block>>
		+ Send
		+ Sync
		+ 'static,
	Client: ApiAccess<Block, ReadOnlyBackend<Block, Db>, Runtime> + 'static,
	NumberFor<Block>: Into<u32> + From<u32> + Unpin,
	Block::Hash: Unpin,
	Block::Header: serde::de::DeserializeOwned,
{
	fn new(config: SystemConfig<Block, Db>, client: Arc<Client>) -> Result<Self> {
		Ok(Self { config, client, _marker: PhantomData })
	}

	async fn work(self) -> Result<()> {
		let actors = Actors::spawn(&self.config).await?;
		let pool = actors.db.send(GetState::Pool).await??.pool();
		let persistent_config = &self.config.persistent_config;
		let actors_future = actors.tick_interval();

		if self.config.control.storage_indexing {
			let runner = self.start_queue(&actors, &persistent_config.task_queue)?;
			let handle = runner.unique_handle()?;
			let mut listener = self.init_listeners(handle.clone()).await?;
			let task_loop = self.storage_index(runner, pool);
			futures::try_join!(task_loop, actors_future)?;
			listener.kill().await?;
		} else {
			actors_future.await?
		};

		Ok(())
	}

	async fn storage_index(
		&self,
		runner: TaskRunner<Block, Block::Hash, Runtime, Client, Db>,
		pool: sqlx::PgPool,
	) -> Result<()> {
		let control_config = self.config.control.clone();
		let mut last = Instant::now();
		let handle = runner.handle().clone();
		task::spawn_blocking(move || loop {
			match runner.run_pending_tasks() {
				Ok(_) => {
					// we don't have any tasks to process. Add more.
					if runner.job_count() == 0 && last.elapsed() > Duration::from_secs(60) {
						// we don't want to restore too often to avoid dups.
						last = Instant::now();
						let handle = task::spawn(Self::restore_missing_storage(
							control_config.clone(),
							pool.clone(),
							handle.clone(),
						));
						if let Err(e) = task::block_on(handle) {
							log::error!("{}", e);
						}
					}
				}
				Err(sa_work_queue::FetchError::Timeout) => log::warn!("Tasks timed out"),
				Err(e) => log::error!("{:?}", e),
			}
		})
		.await
	}

	fn start_queue(
		&self,
		actors: &Actors<Block, Block::Hash, Db>,
		queue: &str,
	) -> Result<TaskRunner<Block, Block::Hash, Runtime, Client, Db>> {
		let env = Environment::<Block, Block::Hash, Runtime, Client, Db>::new(
			self.config.backend().clone(),
			self.client.clone(),
			actors.storage.clone(),
			self.config.tracing_targets.clone(),
		);
		let env = AssertUnwindSafe(env);

		let runner = sa_work_queue::Runner::builder(env, &self.config.control.task_url)
			.register_job::<crate::tasks::execute_block::Job<Block, Runtime, Client, Db>>()
			.num_threads(self.config.runtime.block_workers)
			.queue_name(queue)
			.prefetch(100)
			// times out if tasks don't start execution on the threadpool within timeout.
			.timeout(Duration::from_secs(self.config.control.task_timeout))
			.build()?;

		Ok(runner)
	}

	async fn init_listeners(&self, handle: QueueHandle) -> Result<Listener> {
		Listener::builder(self.config.pg_url(), handle, move |notif, conn, handle| {
			async move {
				let sql_block = queries::get_full_block_by_number(conn, notif.block_num).await?;
				let b = sql_block.into_block_and_spec()?;
				crate::tasks::execute_block::<Block, Runtime, Client, Db>(b.0, PhantomData).enqueue(handle).await?;
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
	/// If any are found, they are re-enqueued.
	async fn restore_missing_storage(config: ControlConfig, pool: sqlx::PgPool, handle: QueueHandle) -> Result<()> {
		let mut conn = pool.acquire().await?;
		let nums = queries::missing_storage_blocks(&mut *conn).await?;
		log::info!("Restoring {} missing storage entries.", nums.len());
		let load: usize = config.max_block_load.try_into()?;
		let mut block_stream = queries::blocks_paginated(&mut *conn, nums.as_slice(), load);
		while let Some(page) = block_stream.next().await {
			let jobs: Vec<crate::tasks::execute_block::Job<Block, Runtime, Client, Db>> =
				BlockModelDecoder::with_vec(page?)?
					.into_iter()
					.map(|b| crate::tasks::execute_block::<Block, Runtime, Client, Db>(b.inner.block, PhantomData))
					.collect();
			sa_work_queue::JobExt::enqueue_batch(&handle, jobs).await?;
		}
		Ok(())
	}
}

#[async_trait::async_trait(?Send)]
impl<Block, Runtime, Client, Db> Archive<Block, Db> for System<Block, Runtime, Client, Db>
where
	Db: ReadOnlyDb + 'static,
	Block: BlockT + Unpin + DeserializeOwned,
	<Block as BlockT>::Hash: Unpin,
	Runtime: ConstructRuntimeApi<Block, Client> + Send + Sync + 'static,
	Runtime::RuntimeApi: BlockBuilderApi<Block>
		+ sp_api::Metadata<Block>
		+ ApiExt<Block, StateBackend = backend::StateBackendFor<ReadOnlyBackend<Block, Db>, Block>>
		+ Send
		+ Sync
		+ 'static,
	Client: ApiAccess<Block, ReadOnlyBackend<Block, Db>, Runtime> + 'static,
	NumberFor<Block>: Into<u32> + From<u32> + Unpin,
	Block::Hash: Unpin,
	Block::Header: serde::de::DeserializeOwned,
{
	fn drive(&mut self) -> Result<()> {
		System::drive(self)?;
		Ok(())
	}

	async fn block_until_stopped(&self) {
		loop {
			Delay::new(std::time::Duration::from_secs(1)).await;
		}
	}

	fn shutdown(self) -> Result<()> {
		let now = std::time::Instant::now();
		if let Some(h) = self.handle {
			task::block_on(async {
				if timeout(Duration::from_secs(1), h.cancel()).await.is_err() {
					log::warn!("shutdown timed out...");
				}
			})
		}
		log::debug!("Shutdown took {:?}", now.elapsed());
		Ok(())
	}

	fn boxed_shutdown(self: Box<Self>) -> Result<()> {
		self.shutdown()
	}

	fn context(&self) -> &SystemConfig<Block, Db> {
		&self.config
	}
}
