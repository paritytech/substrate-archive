// Copyright 2017-2021 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

mod client;

use std::{path::PathBuf, sync::Arc};

use futures::{future::BoxFuture, task::SpawnExt};

use sc_client_api::{
	execution_extensions::{ExecutionExtensions, ExecutionStrategies},
	ExecutionStrategy,
};
use sc_executor::{NativeExecutionDispatch, NativeExecutor, WasmExecutionMethod};
use sc_service::{ClientConfig, LocalCallExecutor};
use sp_api::ConstructRuntimeApi;
use sp_core::traits::SpawnNamed;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};

use substrate_archive_common::ReadOnlyDB;

pub use self::client::{Client, GetMetadata, GetRuntimeVersion};
use crate::{error::BackendError, read_only_backend::ReadOnlyBackend, RuntimeApiCollection};

/// Archive Client Condensed Type
pub type TArchiveClient<TBl, TRtApi, TExecDisp, D> = Client<TFullCallExecutor<TBl, TExecDisp, D>, TBl, TRtApi, D>;

/// Full client call executor type.
type TFullCallExecutor<TBl, TExecDisp, D> = LocalCallExecutor<ReadOnlyBackend<TBl, D>, NativeExecutor<TExecDisp>>;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
	pub block_workers: usize,
	pub wasm_pages: u64,
	pub wasm_runtime_overrides: Option<PathBuf>,
}

impl From<RuntimeConfig> for ClientConfig {
	fn from(config: RuntimeConfig) -> ClientConfig {
		ClientConfig {
			offchain_worker_enabled: false,
			offchain_indexing_api: false,
			wasm_runtime_overrides: config.wasm_runtime_overrides,
		}
	}
}

impl Default for RuntimeConfig {
	fn default() -> RuntimeConfig {
		Self { block_workers: 2, wasm_pages: 512, wasm_runtime_overrides: None }
	}
}

pub fn runtime_api<Block, Runtime, Dispatch, D: ReadOnlyDB + 'static>(
	db: Arc<D>,
	config: RuntimeConfig,
) -> Result<TArchiveClient<Block, Runtime, Dispatch, D>, BackendError>
where
	Block: BlockT,
	Runtime: ConstructRuntimeApi<Block, TArchiveClient<Block, Runtime, Dispatch, D>> + Send + Sync + 'static,
	Runtime::RuntimeApi: RuntimeApiCollection<Block, StateBackend = sc_client_api::StateBackendFor<ReadOnlyBackend<Block, D>, Block>>
		+ Send
		+ Sync
		+ 'static,
	Dispatch: NativeExecutionDispatch + 'static,
	<Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
	let backend = Arc::new(ReadOnlyBackend::new(db, true));

	let executor = NativeExecutor::<Dispatch>::new(
		WasmExecutionMethod::Interpreted,
		Some(config.wasm_pages),
		config.block_workers,
	);
	let executor = LocalCallExecutor::new(backend.clone(), executor, Box::new(TaskExecutor::new()), config.into())?;

	let client = Client::new(backend, executor, ExecutionExtensions::new(execution_strategies(), None))?;
	Ok(client)
}

impl SpawnNamed for TaskExecutor {
	fn spawn_blocking(&self, _: &'static str, fut: BoxFuture<'static, ()>) {
		let _ = self.pool.spawn(fut);
	}

	fn spawn(&self, _: &'static str, fut: BoxFuture<'static, ()>) {
		let _ = self.pool.spawn(fut);
	}
}

#[derive(Debug, Clone)]
pub struct TaskExecutor {
	pool: futures::executor::ThreadPool,
}

impl TaskExecutor {
	fn new() -> Self {
		let pool = futures::executor::ThreadPool::builder().pool_size(1).create().unwrap();
		Self { pool }
	}
}

impl futures::task::Spawn for TaskExecutor {
	fn spawn_obj(&self, future: futures::task::FutureObj<'static, ()>) -> Result<(), futures::task::SpawnError> {
		self.pool.spawn_obj(future)
	}
}

fn execution_strategies() -> ExecutionStrategies {
	ExecutionStrategies {
		syncing: ExecutionStrategy::NativeElseWasm,
		importing: ExecutionStrategy::NativeElseWasm,
		block_construction: ExecutionStrategy::NativeElseWasm,
		offchain_worker: ExecutionStrategy::NativeElseWasm,
		other: ExecutionStrategy::NativeElseWasm,
	}
}
