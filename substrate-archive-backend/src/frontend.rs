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

use serde::Deserialize;
use std::{
	collections::{BTreeMap, HashMap},
	convert::{TryFrom, TryInto},
	path::PathBuf,
	str::FromStr,
	sync::Arc,
};

use sc_client_api::{
	execution_extensions::{ExecutionExtensions, ExecutionStrategies},
	ExecutionStrategy,
};
use sc_executor::{WasmExecutionMethod, WasmExecutor};
use sc_service::{ChainSpec, ClientConfig, LocalCallExecutor, TransactionStorageMode};
use sp_api::ConstructRuntimeApi;
use sp_core::traits::SpawnNamed;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT, NumberFor};

pub use self::client::{Client, GetMetadata};
use crate::{database::ReadOnlyDb, error::BackendError, read_only_backend::ReadOnlyBackend, RuntimeApiCollection};

/// Archive Client Condensed Type
pub type TArchiveClient<TBl, TRtApi, D> = Client<TFullCallExecutor<TBl, D>, TBl, TRtApi, D>;

/// Full client call executor type.
type TFullCallExecutor<TBl, D> =
	LocalCallExecutor<TBl, ReadOnlyBackend<TBl, D>, WasmExecutor<sp_io::SubstrateHostFunctions>>;

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

/// Configuration controls how Archive executes blocks in `execute_block`
/// (in tasks.rs in `substrate-archive/`).
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
	/// code substitutes that should be used for the on chain wasm.
	///
	/// NOTE: Not to be confused with 'wasm_runtime_overrides'. code_substitutes
	/// are included in the chain_spec and primarily for fixing problematic on-chain wasm.
	/// If both are in use, the `wasm_runtime_overrides` takes precedence.
	#[serde(skip)]
	code_substitutes: BTreeMap<String, Vec<u8>>,
	/// Method of storing and retrieving transactions(extrinsics).
	#[serde(skip, default = "default_storage_mode")]
	pub storage_mode: TransactionStorageMode,
}

impl RuntimeConfig {
	/// Set the code substitutes for a chain.
	pub fn set_code_substitutes(&mut self, spec: &dyn ChainSpec) {
		self.code_substitutes = spec.code_substitutes();
	}
}

impl Default for RuntimeConfig {
	fn default() -> RuntimeConfig {
		Self {
			exec_method: ExecutionMethod::Interpreted,
			block_workers: default_block_workers(),
			wasm_pages: None,
			wasm_runtime_overrides: None,
			code_substitutes: Default::default(),
			storage_mode: TransactionStorageMode::BlockBody,
		}
	}
}

// the number of logical cpus in the system
fn default_block_workers() -> usize {
	num_cpus::get()
}

const fn default_storage_mode() -> TransactionStorageMode {
	TransactionStorageMode::BlockBody
}

impl<B> TryFrom<RuntimeConfig> for ClientConfig<B>
where
	B: BlockT,
{
	type Error = BackendError;
	fn try_from(config: RuntimeConfig) -> Result<ClientConfig<B>, BackendError> {
		let wasm_runtime_substitutes = config
			.code_substitutes
			.into_iter()
			.map(|(n, code)| {
				let number = NumberFor::<B>::from_str(&n).map_err(|_| {
					BackendError::Msg(format!(
						"Failed to parse `{}` as block number for code substitutes. \
						 In an old version the key for code substitute was a block hash. \
						 Please update the chain spec to a version that is compatible with your node.",
						n
					))
				})?;
				Ok((number, code))
			})
			.collect::<Result<HashMap<_, _>, BackendError>>()?;

		Ok(ClientConfig {
			offchain_worker_enabled: false,
			offchain_indexing_api: false,
			wasm_runtime_overrides: config.wasm_runtime_overrides,
			// we do not support 'no_genesis', so this value is inconsiquential
			no_genesis: false,
			wasm_runtime_substitutes,
		})
	}
}

/// Main entry to initialize the substrate-archive backend client, used to
/// call into the runtime of the network being indexed (e.g to execute blocks).
pub fn runtime_api<Block, Runtime, D: ReadOnlyDb + 'static>(
	config: RuntimeConfig,
	backend: Arc<ReadOnlyBackend<Block, D>>,
	task_executor: impl SpawnNamed + 'static,
) -> Result<TArchiveClient<Block, Runtime, D>, BackendError>
where
	Block: BlockT,
	Block::Hash: FromStr,
	Runtime: ConstructRuntimeApi<Block, TArchiveClient<Block, Runtime, D>> + Send + Sync + 'static,
	Runtime::RuntimeApi: RuntimeApiCollection<Block, StateBackend = sc_client_api::StateBackendFor<ReadOnlyBackend<Block, D>, Block>>
		+ Send
		+ Sync
		+ 'static,
	<Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
	let executor = WasmExecutor::<sp_io::SubstrateHostFunctions>::new(
		config.exec_method.into(),
		config.wasm_pages,
		config.block_workers,
		None,
		128,
	);
	let executor = LocalCallExecutor::new(backend.clone(), executor, Box::new(task_executor), config.try_into()?)?;
	let client = Client::new(backend, executor, ExecutionExtensions::new(execution_strategies(), None, None))?;
	Ok(client)
}

fn execution_strategies() -> ExecutionStrategies {
	ExecutionStrategies {
		syncing: ExecutionStrategy::AlwaysWasm,
		importing: ExecutionStrategy::AlwaysWasm,
		block_construction: ExecutionStrategy::AlwaysWasm,
		offchain_worker: ExecutionStrategy::AlwaysWasm,
		other: ExecutionStrategy::AlwaysWasm,
	}
}
