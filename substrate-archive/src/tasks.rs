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

//! Background tasks that take their parameters from Postgres, and are either
//! executed on a threadpool or spawned onto the executor.

use std::{marker::PhantomData, panic::AssertUnwindSafe, sync::Arc};

use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use xtra::prelude::*;

use sc_client_api::backend;
use sp_api::{ApiExt, ApiRef, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
	generic::BlockId,
	traits::{Block as BlockT, Header, NumberFor},
};

use substrate_archive_backend::{ApiAccess, ReadOnlyBackend as Backend, ReadOnlyDb};

use crate::{
	actors::StorageAggregator,
	error::{ArchiveError, TracingError},
	types::Storage,
	wasm_tracing::{SpansAndEvents, TraceHandler, Traces},
};

/// The environment passed to each task
pub struct Environment<B, R, C, D>
where
	D: ReadOnlyDb,
	B: BlockT + Unpin,
	B::Hash: Unpin,
{
	// Tracing targets
	// if `Some` will trace the execution of the block
	// and traces will be sent to the [`StorageAggregator`].
	tracing_targets: Option<String>,
	backend: Arc<Backend<B, D>>,
	client: Arc<C>,
	storage: Address<StorageAggregator<B>>,
	_marker: PhantomData<R>,
}

type Env<B, R, C, D> = AssertUnwindSafe<Environment<B, R, C, D>>;
impl<B, R, C, D> Environment<B, R, C, D>
where
	D: ReadOnlyDb,
	B: BlockT + Unpin,
	B::Hash: Unpin,
{
	pub fn new(
		backend: Arc<Backend<B, D>>,
		client: Arc<C>,
		storage: Address<StorageAggregator<B>>,
		tracing_targets: Option<String>,
	) -> Self {
		Self { backend, client, storage, tracing_targets, _marker: PhantomData }
	}
}

pub type StorageKey = Vec<u8>;
pub type StorageValue = Vec<u8>;
pub type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;
pub type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

/// Storage Changes that occur as a result of a block's executions
#[derive(Clone, Debug)]
pub struct BlockChanges<Block: BlockT> {
	/// In memory array of storage values.
	pub storage_changes: StorageCollection,
	/// In memory arrays of storage values for multiple child tries.
	pub child_storage: ChildStorageCollection,
	/// Hash of the block these changes come from
	pub block_hash: Block::Hash,
	pub block_num: NumberFor<Block>,
}

impl<Block> From<BlockChanges<Block>> for Storage<Block>
where
	Block: BlockT,
	NumberFor<Block>: Into<u32>,
{
	fn from(changes: BlockChanges<Block>) -> Storage<Block> {
		use sp_storage::{StorageData, StorageKey};

		let hash = changes.block_hash;
		let num: u32 = changes.block_num.into();

		Storage::new(
			hash,
			num,
			false,
			changes
				.storage_changes
				.into_iter()
				.map(|s| (StorageKey(s.0), s.1.map(StorageData)))
				.collect::<Vec<(StorageKey, Option<StorageData>)>>(),
		)
	}
}

struct BlockExecutor<'a, Block, Api, B>
where
	Block: BlockT,
	Api: BlockBuilderApi<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
	B: backend::Backend<Block>,
{
	api: ApiRef<'a, Api>,
	backend: &'a Arc<B>,
	block: Block,
	id: BlockId<Block>,
}

impl<'a, Block, Api, B> BlockExecutor<'a, Block, Api, B>
where
	Block: BlockT,
	Api: BlockBuilderApi<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
	B: backend::Backend<Block>,
{
	fn new(api: ApiRef<'a, Api>, backend: &'a Arc<B>, block: Block) -> Self {
		let header = block.header();
		let parent_hash = header.parent_hash();
		let id = BlockId::Hash(*parent_hash);

		Self { api, backend, block, id }
	}

	fn block_into_storage(self) -> Result<BlockChanges<Block>, ArchiveError> {
		let header = (&self.block).header();
		let parent_hash = *header.parent_hash();
		let hash = header.hash();
		let num = *header.number();

		let state = self.backend.state_at(self.id)?;

		// Wasm runtime calculates a different number of digest items
		// than what we have in the block
		// We don't do anything with consensus
		// so digest isn't very important (we don't currently index digest items anyway)
		// popping a digest item has no effect on storage changes afaik
		let (mut header, ext) = self.block.deconstruct();
		header.digest_mut().pop();
		let block = Block::new(header, ext);

		self.api.execute_block(&self.id, block)?;
		let storage_changes =
			self.api.into_storage_changes(&state, None, parent_hash).map_err(ArchiveError::ConvertStorageChanges)?;

		Ok(BlockChanges {
			storage_changes: storage_changes.main_storage_changes,
			child_storage: storage_changes.child_storage_changes,
			block_hash: hash,
			block_num: num,
		})
	}
}

#[derive(Debug, Clone)]
pub struct TaskExecutor;

impl futures::task::Spawn for TaskExecutor {
	fn spawn_obj(&self, future: futures::task::FutureObj<'static, ()>) -> Result<(), futures::task::SpawnError> {
		smol::spawn(future).detach();
		Ok(())
	}
}

impl sp_core::traits::SpawnNamed for TaskExecutor {
	fn spawn_blocking(&self, _: &'static str, fut: futures::future::BoxFuture<'static, ()>) {
		smol::spawn(async move { smol::unblock(|| fut).await.await }).detach();
	}

	fn spawn(&self, _: &'static str, fut: futures::future::BoxFuture<'static, ()>) {
		smol::spawn(fut).detach()
	}
}

// FIXME:
// we need PhantomData here so that the proc_macro correctly puts PhantomData into the `Job` struct
// + DeserializeOwned so that the types work.
// This is a little bit wonky (and entirely confusing), could be fixed with a better proc-macro in `coil`
// TODO: We should detect when the chain is behind our node, and not execute blocks in this case.
/// Execute a block, and send it to the database actor
#[coil::background_job]
pub fn execute_block<B, RA, Api, D>(
	env: &Env<B, RA, Api, D>,
	block: B,
	_m: PhantomData<(RA, Api, D)>,
) -> Result<(), coil::PerformError>
where
	D: ReadOnlyDb + 'static,
	B: BlockT + DeserializeOwned + Unpin,
	NumberFor<B>: Into<u32>,
	B::Hash: Unpin,
	RA: ConstructRuntimeApi<B, Api> + Send + Sync + 'static,
	RA::RuntimeApi: BlockBuilderApi<B> + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B, D>, B>>,
	Api: ApiAccess<B, Backend<B, D>, RA> + 'static,
{
	let api = env.client.runtime_api();

	if *block.header().parent_hash() == Default::default() {
		return Ok(());
	}

	let hash = block.header().hash();
	let number = *block.header().number();

	log::debug!(
		"Executing Block: {}:{}, version {}",
		block.header().hash(),
		block.header().number(),
		env.client.runtime_version_at(&BlockId::Hash(block.hash())).map_err(|e| format!("{:?}", e))?.spec_version,
	);
	let span_events = Arc::new(Mutex::new(SpansAndEvents { spans: Vec::new(), events: Vec::new() }));

	let storage = {
		// storage scope
		let handler = env
			.tracing_targets
			.as_ref()
			.map(|t| TraceHandler::new(&t, number.into(), hash.as_ref().to_vec(), span_events.clone()));

		let _guard = handler.map(tracing::subscriber::set_default);

		let now = std::time::Instant::now();
		let block = BlockExecutor::new(api, &env.backend, block).block_into_storage()?;
		log::debug!("Took {:?} to execute block", now.elapsed());

		Storage::from(block)
	};

	// We destroy the Arc and transform the Mutex here in order to avoid additional allocation.
	// The Arc is cloned into the thread-local tracing subscriber in the scope of `storage`, creating
	// 2 strong references. When block execution finishes, storage is collected and that reference is dropped.
	// This allows us to unwrap it here. QED.
	let traces = Arc::try_unwrap(span_events).map_err(|_| TracingError::NoTraceAccess)?.into_inner();

	let now = std::time::Instant::now();
	smol::block_on(env.storage.send(storage))?;
	if !traces.events.is_empty() || !traces.spans.is_empty() {
		let traces = Traces::new(number.into(), hash.as_ref().to_vec(), traces.events, traces.spans);
		smol::block_on(env.storage.send(traces))?;
	}
	log::trace!("Took {:?} to insert & send finished task", now.elapsed());
	Ok(())
}
