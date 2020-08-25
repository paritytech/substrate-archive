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
//! executed on a threadpool or spaned onto the executor.

use sp_runtime::{
    traits::{Block as BlockT, NumberFor},
    generic::BlockId
};
use sp_api::{ConstructRuntimeApi, ApiExt};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sc_client_api::backend;
use super::{
    backend::{ReadOnlyBackend as Backend, ApiAccess, BlockExecutor},
    actors::{ActorPool, DatabaseActor},
};
use std::sync::Arc;
use std::marker::PhantomData;
use xtra::prelude::*;

pub struct Environment<B, RA, Api> 
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + Sync + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA> + 'static,
{
    backend: Arc<Backend<B>>,
    client: Arc<Api>,
    db: Address<ActorPool<DatabaseActor<B>>>,
    _marker: PhantomData<RA>,
}

/// Execute a block, and send it to the database actor
#[coil::background_job]
fn execute_block<B, RA, Api>(env: Environment, block: B) -> Result<(), coil::PerformError> 
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + Sync + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA> + 'static,
{
    let api = env.client.runtime_api();

    if *block.header().parent_hash() == Default::default() {
        return Ok(());
    }

    log::trace!(
        "Executing Block: {}:{}, version {}",
        block.header().hash(),
        block.header().number(),
        client
            .runtime_version_at(&BlockId::Hash(block.header().hash()))?
            .spec_version,
    );
    
    let block = BlockExecutor::new(api, backend, block)?.block_into_storage()?;
    
    smol::block_on(env.send(block))?;
    Ok(())
}
