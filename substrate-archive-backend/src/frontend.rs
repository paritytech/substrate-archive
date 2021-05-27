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
use serde::Deserialize;

use sc_client_api::{
	execution_extensions::{ExecutionExtensions, ExecutionStrategies},
	ExecutionStrategy,
};
use sc_executor::{NativeExecutionDispatch, NativeExecutor, WasmExecutionMethod};
use sc_service::{ClientConfig, LocalCallExecutor};
use sp_api::ConstructRuntimeApi;
use sp_core::traits::SpawnNamed;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};

pub use self::client::{Client, GetMetadata};
use crate::{database::ReadOnlyDb, error::BackendError, read_only_backend::ReadOnlyBackend, RuntimeApiCollection};

/// Archive Client Condensed Type
pub type TArchiveClient<TBl, TRtApi, TExecDisp, D> = Client<TFullCallExecutor<TBl, TExecDisp, D>, TBl, TRtApi, D>;

/// Full client call executor type.
type TFullCallExecutor<TBl, TExecDisp, D> = LocalCallExecutor<TBl, ReadOnlyBackend<TBl, D>, NativeExecutor<TExecDisp>>;

#[derive(Copy, Clone, Debug, Deserialize)]
pub enum ExecutionMethod {
	Interpreted,
	Compiled,
}

impl Default for ExecutionMethod {
	fn default() -> Self {
		Self::Interpreted
	}
}

impl From<ExecutionMethod> for WasmExecutionMethod {
	fn from(method: ExecutionMethod) -> Self {
		match method {
			ExecutionMethod::Interpreted => Self::Interpreted,
			ExecutionMethod::Compiled => Self::Compiled,
		}
	}
}

#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeConfig {
	/// How to execute the runtime code: interpreted (default) or JIT compiled.
	#[serde(default)]
	pub exec_method: ExecutionMethod,
	/// Number of threads to spawn for block execution.
	#[serde(default = "default_block_workers")]
	pub block_workers: usize,
	/// Number of 64KB Heap pages to allocate for wasm execution.
	pub wasm_pages: Option<u64>,
	/// Path to WASM blobs to override the on-chain WASM with (required for state change tracing).
	pub wasm_runtime_overrides: Option<PathBuf>,
}

impl Default for RuntimeConfig {
	fn default() -> RuntimeConfig {
		Self {
			exec_method: ExecutionMethod::Interpreted,
			block_workers: default_block_workers(),
			wasm_pages: None,
			wasm_runtime_overrides: None,
		}
	}
}

// the number of logical cpus in the system
fn default_block_workers() -> usize {
	num_cpus::get()
}

impl<B: BlockT> From<RuntimeConfig> for ClientConfig<B> {
	fn from(config: RuntimeConfig) -> ClientConfig<B> {
		ClientConfig {
			offchain_worker_enabled: false,
			offchain_indexing_api: false,
			wasm_runtime_overrides: config.wasm_runtime_overrides,
			wasm_runtime_substitutes: Default::default(),
		}
	}
}

pub fn runtime_api<Block, Runtime, Dispatch, D: ReadOnlyDb + 'static>(
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

	let executor = NativeExecutor::<Dispatch>::new(config.exec_method.into(), config.wasm_pages, config.block_workers);
	let executor = LocalCallExecutor::new(backend.clone(), executor, Box::new(TaskExecutor::new()), config.into())?;
	let client = Client::new(backend, executor, ExecutionExtensions::new(execution_strategies(), None, None))?;
	Ok(client)
}

#[derive(Clone, Debug)]
struct TaskExecutor {
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

impl SpawnNamed for TaskExecutor {
	fn spawn_blocking(&self, _: &'static str, fut: BoxFuture<'static, ()>) {
		let _ = self.pool.spawn(fut);
	}

	fn spawn(&self, _: &'static str, fut: BoxFuture<'static, ()>) {
		let _ = self.pool.spawn(fut);
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
