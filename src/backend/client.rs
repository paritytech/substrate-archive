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

use crate::types::{ArchiveBackend, Substrate};
use sp_core::traits::CloneableSpawn;
use sp_api::{CallApiAt, ConstructRuntimeApi};
use sc_client_api::{backend::Backend as BackendT, ExecutionStrategy, execution_extensions::{ExecutionStrategies, ExecutionExtensions}};
use sc_executor::{NativeExecutionDispatch, WasmExecutionMethod, NativeExecutor};
use sc_service::{
    config::{
        Configuration, DatabaseConfig, KeystoreConfig, OffchainWorkerConfig, PruningMode, Role,
        RpcMethods, TaskType, TransactionPoolOptions,
    },
    error::Error as ServiceError,
    ChainSpec, TracingReceiver, TFullBackend
};
use sc_transaction_graph::base_pool::Limit;
use crate::types::System;
use sp_runtime::{OpaqueExtrinsic, traits::{Block as BlockT, BlakeTwo256}};
use std::{future::Future, pin::Pin, sync::Arc};

use super::{ChainAccess, RuntimeApiCollection};

// create a macro `new_archive!` to simplify all these type constraints in the archive node library
pub fn client<Block, T, EX, S>(
    db_config: DatabaseConfig,
    spec: S,
) -> Result<Arc<impl ChainAccess<Block>>, ServiceError>
where
    Block: BlockT,
    T: ConstructRuntimeApi<Block, sc_service::TFullClient<Block, T, EX>>,
    T::RuntimeApi: RuntimeApiCollection<Block, StateBackend = sc_client_api::StateBackendFor<TFullBackend<Block>, Block>> + Send + Sync + 'static,
    EX: NativeExecutionDispatch + 'static,
    S: ChainSpec + 'static,
    <T::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
    let db_settings = sc_client_db::DatabaseSettings {
        state_cache_size: 4096,
        state_cache_child_ratio: None,
        pruning: PruningMode::ArchiveAll,
        source: db_config
    };
    let profile = Profile::Native;
    let(client, _) = sc_service::new_client::<_, Block, T::RuntimeApi>(
        db_settings,
        NativeExecutor::<EX>::new(WasmExecutionMethod::Interpreted, None, 8),
        &spec,
        None,
        None,
        ExecutionExtensions::new(profile.into_execution_strategies(), None),
        Box::new(TaskExecutor::new()),
        None,
        Default::default()
    ).expect("should not fail");
    Ok(Arc::new(client))
}

#[derive(Debug, Clone)]
pub struct TaskExecutor {
    pool: futures::executor::ThreadPool
}

impl TaskExecutor {
    fn new() -> Self {
        Self {
            pool: futures::executor::ThreadPool::new()
                .expect("Failed to create executor")
        }
    }
}

impl futures::task::Spawn for TaskExecutor {
    fn spawn_obj(&self, future: futures::task::FutureObj<'static, ()>)
                 -> Result<(), futures::task::SpawnError> {
        self.pool.spawn_obj(future)
    }
}

impl CloneableSpawn for TaskExecutor {
    fn clone(&self) -> Box<dyn CloneableSpawn> {
        Box::new(Clone::clone(self))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Profile {
    Native,
    Wasm,
}

impl Profile {
    fn into_execution_strategies(self) -> ExecutionStrategies {
        match self {
            Profile::Wasm => ExecutionStrategies {
                syncing: ExecutionStrategy::AlwaysWasm,
                importing: ExecutionStrategy::AlwaysWasm,
                block_construction: ExecutionStrategy::AlwaysWasm,
                offchain_worker: ExecutionStrategy::AlwaysWasm,
                other: ExecutionStrategy::AlwaysWasm,
            },
            Profile::Native => ExecutionStrategies {
                syncing: ExecutionStrategy::NativeElseWasm,
                importing: ExecutionStrategy::NativeElseWasm,
                block_construction: ExecutionStrategy::NativeElseWasm,
                offchain_worker: ExecutionStrategy::NativeElseWasm,
                other: ExecutionStrategy::NativeElseWasm,
            }
        }
    }
}
