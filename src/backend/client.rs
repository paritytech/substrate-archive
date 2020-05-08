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

use sc_client_api::{backend::Backend as BackendT, execution_extensions::{ExecutionExtensions, ExecutionStrategies}};
use sp_runtime::traits::Block as BlockT;
use sp_state_machine::backend::Backend as StateMachineBackend;
use sp_api::{ProvideRuntimeApi, ConstructRuntimeApi, CallApiAt};
// use sc_client_db::SyncingCachingState;
use sc_service::{
    ServiceBuilder, GenericChainSpec, TracingReceiver,
    AbstractService, TLightBackend, TLightClient,
    error::Error as ServiceError,
    config::{
        self, DatabaseConfig, PruningMode, TransactionPoolOptions,
        Role, KeystoreConfig, Configuration,
        OffchainWorkerConfig, TaskType,
    },
};
use sc_executor::{NativeExecutor, WasmExecutionMethod, NativeExecutionDispatch};
use sc_transaction_graph::base_pool::Limit;
use std::{sync::Arc, path::PathBuf, pin::Pin, future::Future};
use crate::types::{NotSignedBlock, Substrate};


//FIXME: This currently pulls many substrate dependencies that we don't need IE Transaction pooling etc
// sc-client is in the process of being refactored and transitioned into sc-service
// where a method 'new_client' will create a much 'slimmer' database-backed client
// that won't require defining G and E, a chainspec, or pulling in a async-runtime (async-std in this case)
pub fn client<T: Substrate, Run, Exec, Api, G, E>(db_config: DatabaseConfig, spec: PathBuf)
    -> Result<
        Arc<impl ArchiveClient<NotSignedBlock<T>, Run>>,
        // impl ArchiveClient,
        ServiceError>
where
    G: serde::de::DeserializeOwned + Send + sp_runtime::BuildStorage + serde::ser::Serialize + 'static,
    E: sc_chain_spec::Extension + Send + 'static,
    Exec: NativeExecutionDispatch,
    Run: ConstructRuntimeApi<NotSignedBlock<T>, Api>,
    Api: CallApiAt<
        NotSignedBlock<T>, 
        Error=sp_blockchain::Error, 
        StateBackend=<sc_client_db::Backend<NotSignedBlock<T>> as BackendT<NotSignedBlock<T>>>::State>,
{
    // let native_execution = NativeExecutor::new(WasmExecutionMethod::Compiled, None, 2);
    // let call_executor = LocalCallExecutor::new(backend, native_execution, sp_core::tasks::executor());
/*
    let db_settings = DatabaseSettings {
        state_cache_size: 4096,
        state_cache_child_ratio: None,
        pruning: PruningMode::ArchiveAll,
        source: db_config,
    };
    let executor = NativeExecutor::new(WasmExecutionMethod::Interpreted, None, 10);
    let storage = sp_runtime::Storage::default();
    let (fork_blocks, bad_blocks) = (None, None);
    let extensions = ExecutionExtensions::<NotSignedBlock<T>>::new(ExecutionStrategies::default(), None);
    let spawn_handle = sp_core::tasks::executor();
*/


    let chain_spec = GenericChainSpec::<G, E>::from_json_file(spec).map_err(|e| ServiceError::Other(e))?;
    let config = Configuration {
        impl_name: "substrate-archive",
        impl_version: env!("CARGO_PKG_VERSION"),
        role: Role::Light,
        task_executor: task_executor(),
        transaction_pool: TransactionPoolOptions {
            ready: Limit {
                count: 10,
                total_bytes: 32
            },
            future: Limit {
                count: 10,
                total_bytes: 32
            },
            reject_future_transactions: true
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
            indexing_enabled: false
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

    // T Block,  T Runtime Api (ConstructRuntimeApi), NativeExecutionDispatch
    Ok(Arc::new(sc_service::new_full_client(&config)?))
    // let builder = ServiceBuilder::new_light(config)?;
    // let client = builder.client().clone();
    // Ok((builder.build()?, client))
}

pub fn task_executor() -> Arc<dyn Fn(Pin<Box<dyn Future<Output =()> + Send>>, TaskType) + Send + Sync> {
   
    Arc::new(
        move |fut, task_type| {
            match task_type {
                TaskType::Async => {
                    async_std::task::spawn(fut);
                },
                TaskType::Blocking => {
                    // FIXME: use `spawn_blocking`
                    async_std::task::block_on(fut)
                }
            }
        }
    )
}



/// Archive client abstraction, this super trait only pulls in functionality required for
/// substrate-archive internals
/// taken from polkadot::service::client
pub trait ArchiveClient<Block, Runtime>:
    Sized + Send + Sync
    + ProvideRuntimeApi<Block, Api = Runtime::RuntimeApi>
    + CallApiAt<
        Block,
        Error = sp_blockchain::Error,
        StateBackend = <sc_client_db::Backend<Block> as BackendT<Block>>::State
    >
    where
        Block: BlockT,
        // Backend: BackendT<Block>,
        Runtime: ConstructRuntimeApi<Block, Self>
{}

impl<Block, Runtime, Client> ArchiveClient<Block, Runtime> for Client
    where
        Block: BlockT,
        Runtime: ConstructRuntimeApi<Block, Self>,
        // Backend: BackendT<Block>,
        Client: /* BlockchainEvents<Block> + */ ProvideRuntimeApi<Block, Api = Runtime::RuntimeApi>
            + Sized + Send + Sync
            + CallApiAt<
                Block,
                Error = sp_blockchain::Error,
                StateBackend = <sc_client_db::Backend<Block> as BackendT<Block>>::State
            >
{}
