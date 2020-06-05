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

use sc_client_api::execution_extensions::{ExecutionExtensions, ExecutionStrategies};
use sc_executor::{NativeExecutionDispatch, NativeExecutor, WasmExecutionMethod};
use sc_service::{
    config::{DatabaseConfig, PruningMode},
    error::Error as ServiceError,
    ChainSpec, TFullBackend,
};
use sp_api::ConstructRuntimeApi;
use sp_core::traits::CloneableSpawn;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};
use std::sync::Arc;

use super::{ChainAccess, RuntimeApiCollection};

// create a macro `new_archive!` to simplify all these type constraints in the archive node library
pub fn client<Block, Runtime, Dispatch, Spec>(
    db_config: DatabaseConfig,
    spec: Spec,
) -> Result<Arc<impl ChainAccess<Block>>, ServiceError>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, sc_service::TFullClient<Block, Runtime, Dispatch>>
        + Send
        + Sync
        + 'static,
    // Runtime::RuntimeApi: RuntimeApiCollection<
    //        Block,
    //        StateBackend = sc_client_api::StateBackendFor<TFullBackend<Block>, Block>,
    //    > + Send
    //    + Sync
    //    + 'static,
    Dispatch: NativeExecutionDispatch + 'static,
    Spec: ChainSpec + 'static,
    // <Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
    let db_settings = sc_client_db::DatabaseSettings {
        state_cache_size: 4096,
        state_cache_child_ratio: None,
        pruning: PruningMode::ArchiveAll,
        source: db_config,
    };

    let (client, _) = sc_service::new_client::<_, Block, Runtime>(
        db_settings,
        NativeExecutor::<Dispatch>::new(WasmExecutionMethod::Interpreted, None, 8),
        &spec,
        None,
        None,
        ExecutionExtensions::new(ExecutionStrategies::default(), None),
        Box::new(TaskExecutor::new()),
        None,
        Default::default(),
    )
    .expect("client instantiation failed");

    Ok(Arc::new(client))
}

#[derive(Debug, Clone)]
pub struct TaskExecutor {
    pool: futures::executor::ThreadPool,
}

impl TaskExecutor {
    fn new() -> Self {
        Self {
            pool: futures::executor::ThreadPool::new().expect("Failed to create executor"),
        }
    }
}

impl futures::task::Spawn for TaskExecutor {
    fn spawn_obj(
        &self,
        future: futures::task::FutureObj<'static, ()>,
    ) -> Result<(), futures::task::SpawnError> {
        self.pool.spawn_obj(future)
    }
}

impl CloneableSpawn for TaskExecutor {
    fn clone(&self) -> Box<dyn CloneableSpawn> {
        Box::new(Clone::clone(self))
    }
}
