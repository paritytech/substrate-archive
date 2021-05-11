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
use tracing::{dispatcher, Dispatch};
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

// convenience struct for unwrapping parameters needed for block execution
struct BlockPrep<Block, S, H, N> {
	block: Block,
	state: S,
	hash: H,
	parent_hash: H,
	number: N,
}

impl<'a, Block, Api, B> BlockExecutor<'a, Block, Api, B>
where
	Block: BlockT,
	NumberFor<Block>: Into<u32>,
	Api: BlockBuilderApi<Block> + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
	B: backend::Backend<Block>,
{
	fn new(api: ApiRef<'a, Api>, backend: &'a Arc<B>, block: Block) -> Self {
		let header = block.header();
		let parent_hash = header.parent_hash();
		let id = BlockId::Hash(*parent_hash);

		Self { api, backend, block, id }
	}

	fn prepare_block(
		block: Block,
		backend: &B,
		id: &BlockId<Block>,
	) -> Result<BlockPrep<Block, B::State, Block::Hash, NumberFor<Block>>, ArchiveError> {
		let header = block.header();
		let parent_hash = *header.parent_hash();
		let hash = header.hash();
		let number = *header.number();

		let state = backend.state_at(*id)?;

		// Wasm runtime calculates a different number of digest items
		// than what we have in the block
		// We don't do anything with consensus
		// so digest isn't very important (we don't currently index digest items anyway)
		// popping a digest item has no effect on storage changes afaik
		let (mut header, ext) = block.deconstruct();
		header.digest_mut().pop();
		Ok(BlockPrep { block: Block::new(header, ext), state, hash, parent_hash, number })
	}

	fn block_into_storage(self) -> Result<BlockChanges<Block>, ArchiveError> {
		let BlockPrep { block, state, hash, parent_hash, number } =
			Self::prepare_block(self.block, &self.backend, &self.id)?;

		self.api.execute_block(&self.id, block)?;
		let storage_changes =
			self.api.into_storage_changes(&state, None, parent_hash).map_err(ArchiveError::ConvertStorageChanges)?;

		Ok(BlockChanges {
			storage_changes: storage_changes.main_storage_changes,
			child_storage: storage_changes.child_storage_changes,
			block_hash: hash,
			block_num: number,
		})
	}

	fn execute_with_tracing(self, targets: &str) -> Result<(BlockChanges<Block>, Traces), ArchiveError> {
		let BlockExecutor { block, backend, id, api } = self;
		let BlockPrep { block, state, hash, parent_hash, number } = Self::prepare_block(block, &backend, &id)?;

		let span_events = Arc::new(Mutex::new(SpansAndEvents { spans: Vec::new(), events: Vec::new() }));
		{
			let handler = TraceHandler::new(&targets, number.into(), hash.as_ref().to_vec(), span_events.clone());
			let dispatch = Dispatch::new(handler);

			let dispatcher_span = tracing::debug_span!(
				target: "state_tracing",
				"execute_block",
				extrinsics_len = block.extrinsics().len()
			);
			let _guard = dispatcher_span.enter();
			let res = dispatcher::with_default(&dispatch, || {
				let span = tracing::info_span!("block_trace", "trace_block");
				let _enter = span.enter();
				api.execute_block(&id, block)
			});
			if res.is_err() {
				println!("{:?}", res);
				panic!("FUCK");
			}
		}

		let changes =
			api.into_storage_changes(&state, None, parent_hash).map_err(ArchiveError::ConvertStorageChanges)?;

		let changes = BlockChanges {
			storage_changes: changes.main_storage_changes,
			child_storage: changes.child_storage_changes,
			block_hash: hash,
			block_num: number,
		};
		log::debug!("CHANGES LENGTH: {}", changes.storage_changes.len());
		// We destroy the Arc and transform the Mutex here in order to avoid additional allocation.
		// The Arc is cloned into the thread-local tracing subscriber in the scope of `storage`, creating
		// 2 strong references. When block execution finishes, storage is collected and that reference is dropped.
		// This allows us to unwrap it here. QED.
		let traces = Arc::try_unwrap(span_events).map_err(|_| TracingError::NoTraceAccess)?.into_inner();
		let traces = Traces::new(number.into(), hash.as_ref().to_vec(), traces.events, traces.spans);
		Ok((changes, traces))
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

	log::debug!(
		"Executing Block: {}:{}, version {}",
		block.header().hash(),
		block.header().number(),
		env.client.runtime_version_at(&BlockId::Hash(block.hash())).map_err(|e| format!("{:?}", e))?.spec_version,
	);

	let block = BlockExecutor::new(api, &env.backend, block);

	let now = std::time::Instant::now();
	let (storage, traces) = if let Some(targets) = env.tracing_targets.as_ref() {
		let res = block.execute_with_tracing(targets);
		if res.is_err() {
			println!("{:?}", res);
			panic!("ERR");
		} else {
			res?
		}
	} else {
		(block.block_into_storage()?, Default::default())
	};
	log::debug!("Took {:?} to execute block", now.elapsed());

	let now = std::time::Instant::now();
	smol::block_on(env.storage.send(Storage::from(storage)))?;
	if !traces.events.is_empty() || !traces.spans.is_empty() {
		log::info!("Sending {} events and {} spans", traces.events.len(), traces.spans.len());
		smol::block_on(env.storage.send(traces))?;
	}
	log::trace!("Took {:?} to insert & send finished task", now.elapsed());
	Ok(())
}

/// these tests require a synced polkadot database with --pruning=archive to run
#[cfg(test)]
mod test {
	use super::*;
	use test_common::*;
	use sp_api::ProvideRuntimeApi;
	use anyhow::Error;
	const targets: &str = "wasm_tracing,pallet,frame,state";

	#[test]
	fn trace_block_v0_8_28() -> Result<(), Error> {
		let (client, backend) = get_dot_runtime_api(1, 128).unwrap();
		let client = Arc::new(client);
		let blocks = blocks_v25().unwrap();
		let api = client.runtime_api();
		let (block, _) = blocks[0].clone().into_block_and_spec()?;
		let executor = BlockExecutor::new(api, &backend, block);
		let (_, traces) = executor.execute_with_tracing(targets).unwrap();
		println!("{:?}", traces);
		Ok(())
	}
}
