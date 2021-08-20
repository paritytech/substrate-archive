// Copyright 2017-2021 Parity Technologies (UK) Ltd.
// This fin/le is part of substrate-archive.

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

mod workers;

use std::{convert::TryInto, marker::PhantomData, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use async_std::{
	future::timeout,
	task::{self, JoinHandle},
};
use futures::{future, FutureExt, StreamExt};
use futures_timer::Delay;
use sa_work_queue::{Job as _, QueueHandle, Runner};
use serde::{de::DeserializeOwned, Deserialize};
use xtra::{prelude::*, spawn::AsyncStd};

use flume::Receiver;
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};

use substrate_archive_backend::{ApiAccess, Meta, ReadOnlyBackend, ReadOnlyDb, RuntimeConfig};

use self::workers::{
	blocks::{Crawl, ReIndex},
	storage_aggregator::{SendStorage, SendTraces},
	GetState,
};
pub use self::workers::{BlocksIndexer, DatabaseActor, StorageAggregator};
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
pub struct SystemConfig<B, D> {
	pub backend: Arc<ReadOnlyBackend<B, D>>,
	pub pg_url: String,
	pub meta: Meta<B>,
	pub control: ControlConfig,
	pub runtime: RuntimeConfig,
	pub tracing_targets: Option<String>,
}

impl<B, D> Clone for SystemConfig<B, D> {
	fn clone(&self) -> SystemConfig<B, D> {
		SystemConfig {
			backend: Arc::clone(&self.backend),
			pg_url: self.pg_url.clone(),
			meta: self.meta.clone(),
			control: self.control.clone(),
			runtime: self.runtime.clone(),
			tracing_targets: self.tracing_targets.clone(),
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
	/// RabbitMq URL. default: `http://localhost:5672`
	#[serde(default = "default_task_url")]
	pub(crate) task_url: String,
}

impl Default for ControlConfig {
	fn default() -> Self {
		Self {
			task_timeout: default_task_timeout(),
			max_block_load: default_max_block_load(),
			task_url: default_task_url(),
		}
	}
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

impl<B: BlockT + Unpin, D: ReadOnlyDb> SystemConfig<B, D>
where
	B::Hash: Unpin,
{
	pub fn new(
		backend: Arc<ReadOnlyBackend<B, D>>,
		pg_url: String,
		meta: Meta<B>,
		control: ControlConfig,
		runtime: RuntimeConfig,
		tracing_targets: Option<String>,
	) -> Self {
		Self { backend, pg_url, meta, control, runtime, tracing_targets }
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

struct Actors<Block: Send + Sync + 'static, H: Send + Sync + 'static, Db: Send + Sync + 'static> {
	storage: Address<workers::StorageAggregator<H>>,
	blocks: Address<workers::BlocksIndexer<Block, Db>>,
	metadata: Address<workers::MetadataActor<Block>>,
	db: Address<DatabaseActor>,
}

impl<B: Send + Sync + 'static, H: Send + Sync + 'static, D: Send + Sync + 'static> Clone for Actors<B, H, D> {
	fn clone(&self) -> Self {
		Self {
			storage: self.storage.clone(),
			blocks: self.blocks.clone(),
			metadata: self.metadata.clone(),
			db: self.db.clone(),
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
		let db = workers::DatabaseActor::new(conf.pg_url().into()).await?.create(None).spawn(&mut AsyncStd);
		let storage = workers::StorageAggregator::new(db.clone()).create(None).spawn(&mut AsyncStd);
		let metadata =
			workers::MetadataActor::new(db.clone(), conf.meta().clone()).await?.create(None).spawn(&mut AsyncStd);
		let blocks = workers::BlocksIndexer::new(conf, db.clone(), metadata.clone()).create(None).spawn(&mut AsyncStd);

		Ok(Actors { storage, blocks, metadata, db })
	}

	// Run a future that sends actors a signal to progress once the previous
	// messages have been processed.
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
				);
				if let (Err(_), Err(_), Err(_)) = future::join3(fut.0, fut.1, fut.2).await {
					break;
				}
			}
		});
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

impl<B, R, C, D> System<B, R, C, D>
where
	D: ReadOnlyDb + 'static,
	B: BlockT + Unpin + DeserializeOwned,
	R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
	R::RuntimeApi: BlockBuilderApi<B>
		+ sp_api::Metadata<B>
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
		client: Arc<C>,
		config: SystemConfig<B, D>,
	) -> Result<Self> {
		Ok(Self { handle: None, config, client, _marker: PhantomData })
	}

	fn drive(&mut self) -> Result<()> {
		let instance = SystemInstance::new(self.config.clone(), self.client.clone())?;
		// task::block_on(instance.work())?;
		let handle = task::spawn(instance.work());
		self.handle.replace(handle);
		Ok(())
	}
}

pub struct SystemInstance<B, R, D, C> {
	config: SystemConfig<B, D>,
	client: Arc<C>,
	_marker: PhantomData<R>,
}

impl<B, R, D, C> SystemInstance<B, R, D, C>
where
	D: ReadOnlyDb + 'static,
	B: BlockT + Unpin + DeserializeOwned,
	R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
	R::RuntimeApi: BlockBuilderApi<B>
		+ sp_api::Metadata<B>
		+ ApiExt<B, StateBackend = backend::StateBackendFor<ReadOnlyBackend<B, D>, B>>
		+ Send
		+ Sync
		+ 'static,
	C: ApiAccess<B, ReadOnlyBackend<B, D>, R> + 'static,
	NumberFor<B>: Into<u32> + From<u32> + Unpin,
	B::Hash: Unpin,
	B::Header: serde::de::DeserializeOwned,
{
	fn new(config: SystemConfig<B, D>, client: Arc<C>) -> Result<Self> {
		Ok(Self { config, client, _marker: PhantomData })
	}

	async fn work(self) -> Result<()> {
		let config = self.config.clone();
		let actors = Actors::spawn(&self.config).await?;
		let pool = actors.db.send(GetState::Pool).await??.pool();
		let persistent_config = PersistentConfig::fetch_and_update(&mut *pool.acquire().await?).await?;

		actors.tick_interval().await?;

		let runner = self.start_queue(&actors, &persistent_config.task_queue)?;
		let handle = runner.unique_handle()?;
		let mut listener = self.init_listeners(handle.clone()).await?;
		let (storage_tx, storage_rx) = flume::bounded(1);
		let storage_handle = Self::restore_missing_storage(storage_rx, config.clone(), pool.clone(), handle.clone());

		let task_loop = task::spawn_blocking(move || loop {
			match runner.run_pending_tasks() {
				Ok(_) => {
					// we don't have any tasks to process. Add more and sleep.
					if runner.job_count() < config.control.max_block_load as usize {
						let _ = storage_tx.try_send(());
					}
					std::thread::sleep(Duration::from_millis(100));
				}
				Err(sa_work_queue::FetchError::Timeout) => log::warn!("Tasks timed out"),
				Err(e) => log::error!("{:?}", e),
			}
		});

		futures::join!(storage_handle, task_loop).0?;
		listener.kill().await?;
		Ok(())
	}

    #[allow(clippy::type_complexity)]
	fn start_queue(
		&self,
		actors: &Actors<B, B::Hash, D>,
		queue: &str,
	) -> Result<Runner<AssertUnwindSafe<Environment<B, B::Hash, R, C, D>>>> {
		let env = Environment::<B, B::Hash, R, C, D>::new(
			self.config.backend().clone(),
			self.client.clone(),
			actors.storage.clone(),
			self.config.tracing_targets.clone(),
		);
		let env = AssertUnwindSafe(env);

		let runner = sa_work_queue::Runner::builder(env, &self.config.control.task_url)
			.register_job::<crate::tasks::execute_block::Job<B, R, C, D>>()
			.num_threads(self.config.runtime.block_workers)
			.queue_name(queue)
			.prefetch(5000)
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
				crate::tasks::execute_block::<B, R, C, D>(b.0, PhantomData).enqueue(handle).await?;
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
	async fn restore_missing_storage(
		signal: Receiver<()>,
		conf: SystemConfig<B, D>,
		pool: sqlx::PgPool,
		handle: QueueHandle,
	) -> Result<()> {
		loop {
			let _ = signal.recv_async().await; // signal to restore storage
			let mut conn0 = pool.acquire().await?;
			let nums = queries::missing_storage_blocks(&mut *conn0).await?;
			log::info!("Restoring {} missing storage entries.", nums.len());
			let mut block_stream =
				queries::blocks_paginated(&mut *conn0, nums.as_slice(), conf.control.max_block_load.try_into()?);
			while let Some(Ok(page)) = block_stream.next().await {
				let jobs: Vec<crate::tasks::execute_block::Job<B, R, C, D>> = BlockModelDecoder::with_vec(page)?
					.into_iter()
					.map(|b| crate::tasks::execute_block::<B, R, C, D>(b.inner.block, PhantomData))
					.collect();
				sa_work_queue::JobExt::enqueue_batch(&handle, jobs).await?;
			}
		}
	}
}

#[async_trait::async_trait(?Send)]
impl<B, R, C, D> Archive<B, D> for System<B, R, C, D>
where
	D: ReadOnlyDb + 'static,
	B: BlockT + Unpin + DeserializeOwned,
	<B as BlockT>::Hash: Unpin,
	R: ConstructRuntimeApi<B, C> + Send + Sync + 'static,
	R::RuntimeApi: BlockBuilderApi<B>
		+ sp_api::Metadata<B>
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

	fn context(&self) -> &SystemConfig<B, D> {
		&self.config
	}
}
