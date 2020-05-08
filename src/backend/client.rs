// Copyright 2017-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

use crate::types::{ArchiveBackend, NotSignedBlock, Substrate};
use sc_client_api::{
    backend::{Backend as BackendT, StorageProvider},
    client::BlockBackend,
    execution_extensions::ExecutionStrategies,
};
use sc_executor::{NativeExecutionDispatch, WasmExecutionMethod};
use sc_service::{
    config::{
        Configuration, DatabaseConfig, KeystoreConfig, OffchainWorkerConfig, PruningMode, Role,
        TaskType, TransactionPoolOptions,
    },
    error::Error as ServiceError,
    GenericChainSpec, TracingReceiver,
};
use sc_transaction_graph::base_pool::Limit;
use sp_blockchain::HeaderBackend;
use sp_runtime::traits::Block as BlockT;
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

// functions 'client' and 'internal client' are split purely to make it easier conceptualizing type
// defs

// create a macro `new_archive!` to simplify all these type constraints in the archive node library
pub fn client<T: Substrate, G, E, RA, EX>(
    db_config: DatabaseConfig,
    spec: PathBuf,
) -> Result<Arc<impl ChainAccess<NotSignedBlock<T>>>, ServiceError>
where
    G: serde::de::DeserializeOwned
        + Send
        + sp_runtime::BuildStorage
        + serde::ser::Serialize
        + 'static,
    E: sc_chain_spec::Extension + Send + 'static,
    RA: Send + Sync + 'static,
    EX: NativeExecutionDispatch + 'static,
{
    let chain_spec =
        GenericChainSpec::<G, E>::from_json_file(spec).map_err(|e| ServiceError::Other(e))?;
    let config = Configuration {
        impl_name: "substrate-archive",
        impl_version: env!("CARGO_PKG_VERSION"),
        role: Role::Light,
        task_executor: task_executor(),
        transaction_pool: TransactionPoolOptions {
            ready: Limit {
                count: 10,
                total_bytes: 32,
            },
            future: Limit {
                count: 10,
                total_bytes: 32,
            },
            reject_future_transactions: true,
        },
        network: super::util::constrained_network_config(),
        keystore: KeystoreConfig::InMemory,
        database: db_config,
        state_cache_size: 1_048_576, // 1 MB of cache
        state_cache_child_ratio: Some(20),
        pruning: PruningMode::ArchiveAll,
        wasm_method: WasmExecutionMethod::Interpreted,
        execution_strategies: ExecutionStrategies::default(),
        unsafe_rpc_expose: false,
        rpc_http: None,
        rpc_ws: None,
        rpc_ws_max_connections: None,
        rpc_cors: None,
        prometheus_config: None,
        telemetry_endpoints: None,
        telemetry_external_transport: None,
        default_heap_pages: None,
        offchain_worker: OffchainWorkerConfig {
            enabled: false,
            indexing_enabled: false,
        },
        force_authoring: false,
        disable_grandpa: false,
        dev_key_seed: None,
        tracing_targets: None,
        tracing_receiver: TracingReceiver::Log,
        max_runtime_instances: 8,
        announce_block: false,
        chain_spec: Box::new(chain_spec),
    };

    Ok(internal_client::<
        NotSignedBlock<T>,
        ArchiveBackend<T>,
        RA,
        EX,
    >(&config)?)
}

// FIXME: This currently pulls many substrate dependencies that we don't need IE Transaction pooling etc
// sc-client is in the process of being refactored and transitioned into sc-service
// where a method 'new_client' will create a much 'slimmer' database-backed client
// that won't require defining G and E, a chainspec, or pulling in a async-runtime (async-std in this case)
// We could get away with not needing a RuntimeApi or Execution Dispatch, so these become
// unnecessary things the user is forced to pass into the Archive Node
// RuntimeApi could be useful, however, but only for getting the metadata for a block
fn internal_client<B, Backend, RA, EX>(
    config: &Configuration,
) -> Result<Arc<impl ChainAccess<B>>, ServiceError>
where
    B: BlockT,
    // B::Hash: FromStr,
    RA: Send + Sync + 'static,
    EX: NativeExecutionDispatch + 'static,
    Backend: BackendT<B>,
{
    Ok(Arc::new(sc_service::new_full_client::<B, RA, EX>(&config)?))
}

/// Really simple task executor using async-std
pub fn task_executor(
) -> Arc<dyn Fn(Pin<Box<dyn Future<Output = ()> + Send>>, TaskType) + Send + Sync> {
    Arc::new(move |fut, task_type| {
        match task_type {
            TaskType::Async => {
                async_std::task::spawn(fut);
            }
            TaskType::Blocking => {
                // FIXME: use `spawn_blocking`
                async_std::task::block_on(fut)
            }
        }
    })
}

/// Super trait defining what access the archive node needs to siphon data from running chains
pub trait ChainAccess<Block>:
    StorageProvider<Block, sc_client_db::Backend<Block>> + BlockBackend<Block> + HeaderBackend<Block>
where
    Block: BlockT,
{
}

impl<T, Block> ChainAccess<Block> for T
where
    Block: BlockT,
    T: StorageProvider<Block, sc_client_db::Backend<Block>>
        + BlockBackend<Block>
        + HeaderBackend<Block>,
{
}
