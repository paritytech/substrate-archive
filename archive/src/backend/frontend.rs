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
pub use self::client::Client;
use sc_client_api::{
    execution_extensions::{ExecutionExtensions, ExecutionStrategies},
    ExecutionStrategy,
};
// use sc_client_db::Backend;
use sc_executor::{NativeExecutionDispatch, NativeExecutor, WasmExecutionMethod};
/*
use sc_service::{
    config::{DatabaseConfig, PruningMode},
    error::Error as ServiceError,
    ChainSpec, TFullBackend,
};
*/
use self::executor::ArchiveExecutor;
use crate::{backend::database::ReadOnlyDatabase, error::Error as ArchiveError};
use sc_chain_spec::ChainSpec;
use sp_api::ConstructRuntimeApi;
use sp_core::{tasks::executor as task_executor, traits::CloneableSpawn};
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};
use std::sync::Arc;

use super::{ApiAccess, ReadOnlyBackend, RuntimeApiCollection};

/// Archive Client Condensed Type
pub type TArchiveClient<TBl, TRtApi, TExecDisp> =
    Client<TFullCallExecutor<TBl, TExecDisp>, TBl, TRtApi>;

/// Full client call executor type.
type TFullCallExecutor<TBl, TExecDisp> =
    self::executor::ArchiveExecutor<ReadOnlyBackend<TBl>, NativeExecutor<TExecDisp>>;

pub fn runtime_api<Block, Runtime, Dispatch, Spec>(
    db: ReadOnlyDatabase,
    spec: Spec,
) -> Result<impl ApiAccess<Block, ReadOnlyBackend<Block>, Runtime>, ArchiveError>
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
    Spec: ChainSpec + 'static,
    <Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
    // FIXME: prefix keys

    let backend = Arc::new(ReadOnlyBackend::new(db, true));

    let executor =
        NativeExecutor::<Dispatch>::new(WasmExecutionMethod::Interpreted, Some(4096), 16);
    let executor = ArchiveExecutor::new(backend.clone(), executor, task_executor());

    let client = Client::new(
        backend,
        executor,
        ExecutionExtensions::new(execution_strategies(), None),
    )?;
    Ok(client)
}

fn execution_strategies() -> ExecutionStrategies {
    ExecutionStrategies {
        syncing: ExecutionStrategy::NativeElseWasm,
        importing: ExecutionStrategy::NativeElseWasm,
        block_construction: ExecutionStrategy::NativeElseWasm,
        offchain_worker: ExecutionStrategy::NativeWhenPossible,
        other: ExecutionStrategy::NativeElseWasm,
    }
}
