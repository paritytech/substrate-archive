// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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
mod executor;
pub use self::client::{Client, GetMetadata, GetRuntimeVersion};
use sc_client_api::{
    execution_extensions::{ExecutionExtensions, ExecutionStrategies},
    ExecutionStrategy,
};
// use sc_client_db::Backend;
use self::executor::ArchiveExecutor;
use crate::{backend::database::ReadOnlyDatabase, error::Error as ArchiveError};
use futures::{task::SpawnExt, Future};
use sc_executor::{NativeExecutionDispatch, NativeExecutor, WasmExecutionMethod};
use sp_api::ConstructRuntimeApi;
use sp_core::traits::SpawnNamed;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};
use std::sync::Arc;

use super::{ReadOnlyBackend, RuntimeApiCollection};

/// Archive Client Condensed Type
pub type TArchiveClient<TBl, TRtApi, TExecDisp> =
    Client<TFullCallExecutor<TBl, TExecDisp>, TBl, TRtApi>;

/// Full client call executor type.
type TFullCallExecutor<TBl, TExecDisp> =
    self::executor::ArchiveExecutor<ReadOnlyBackend<TBl>, NativeExecutor<TExecDisp>>;

pub fn runtime_api<Block, Runtime, Dispatch>(
    db: Arc<ReadOnlyDatabase>,
    block_workers: usize,
    wasm_pages: u64,
) -> Result<TArchiveClient<Block, Runtime, Dispatch>, ArchiveError>
where
    Block: BlockT,
    Runtime: ConstructRuntimeApi<Block, TArchiveClient<Block, Runtime, Dispatch>>
        + Send
        + Sync
        + 'static,
    Runtime::RuntimeApi: RuntimeApiCollection<
            Block,
            StateBackend = sc_client_api::StateBackendFor<ReadOnlyBackend<Block>, Block>,
        > + Send
        + Sync
        + 'static,
    Dispatch: NativeExecutionDispatch + 'static,
    <Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
    let backend = Arc::new(ReadOnlyBackend::new(db, true));

    let executor = NativeExecutor::<Dispatch>::new(
        WasmExecutionMethod::Interpreted,
        Some(wasm_pages),
        block_workers as usize,
    );

    let executor = ArchiveExecutor::new(backend.clone(), executor, TaskExecutor::new());

    let client = Client::new(
        backend,
        executor,
        ExecutionExtensions::new(execution_strategies(), None),
    )?;
    Ok(client)
}

impl SpawnNamed for TaskExecutor {
    fn spawn(
        &self,
        _: &'static str,
        fut: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) {
        let _ = self.pool.spawn(fut);
    }

    fn spawn_blocking(
        &self,
        _: &'static str,
        fut: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) {
        let _ = self.pool.spawn(fut);
    }
}

#[derive(Debug, Clone)]
pub struct TaskExecutor {
    pool: futures::executor::ThreadPool,
}

impl TaskExecutor {
    fn new() -> Self {
        let pool = futures::executor::ThreadPool::builder()
            .pool_size(1)
            .create()
            .unwrap();
        Self { pool }
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

fn execution_strategies() -> ExecutionStrategies {
    ExecutionStrategies {
        syncing: ExecutionStrategy::NativeElseWasm,
        importing: ExecutionStrategy::NativeElseWasm,
        block_construction: ExecutionStrategy::NativeElseWasm,
        offchain_worker: ExecutionStrategy::NativeWhenPossible,
        other: ExecutionStrategy::AlwaysWasm,
    }
}
