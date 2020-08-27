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
    traits::{Block as BlockT, NumberFor, Header},
    generic::BlockId
};
use sp_api::{ConstructRuntimeApi, ApiExt};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sc_client_api::backend;
use serde::de::DeserializeOwned;
use crate::types::Storage;
use super::{
    backend::{ReadOnlyBackend as Backend, ApiAccess, BlockExecutor},
    actors::{ActorPool, DatabaseActor},
};
use std::sync::Arc;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use xtra::prelude::*;

/// The environment passed to each task
pub struct Environment<B, R, C> 
where
    B: BlockT,
{
    backend: Arc<Backend<B>>,
    client: Arc<C>,
    db: Address<ActorPool<DatabaseActor<B>>>,
    _marker: PhantomData<R>,
}

type Env<B, R,C> = AssertUnwindSafe<Environment<B, R,C>>;
type DbActor<B> = Address<ActorPool<DatabaseActor<B>>>;
impl<B, R, C> Environment<B, R, C> 
where
    B: BlockT,
{
    pub fn new(backend: Arc<Backend<B>>, client: Arc<C>, db: DbActor<B>) -> Self {
    
        Self {
            backend,
            client,
            db,
            _marker: PhantomData,
        }
    }
}

// we need PhantomData here so that the proc_macro correctly puts PhantomData into the `Job` struct
// + DeserializeOwned a little bit wonky, could be fixed with a better proc-macro in `coil`
/// Execute a block, and send it to the database actor
#[coil::background_job]
pub fn execute_block<B, RA, Api>(env: &Env<B, RA, Api>, block: B, _m: PhantomData<(RA, Api)>) -> Result<(), coil::PerformError> 
where
    B: BlockT + DeserializeOwned,
    NumberFor<B>: Into<u32>,
    B::Hash: Unpin,
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
        env.client
            .runtime_version_at(&BlockId::Hash(block.header().hash()))
            .map_err(|e| format!("{:?}", e))?
            .spec_version,
    );
    
    let block = BlockExecutor::new(api, &env.backend, block)?.block_into_storage()?;
    
    let storage = Storage::from(block);
    env.db.do_send(storage.into())?;
    Ok(())
}
