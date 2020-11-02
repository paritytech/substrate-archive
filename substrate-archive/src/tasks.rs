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

use super::actors::StorageAggregator;
use sc_client_api::backend;
use serde::de::DeserializeOwned;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header, NumberFor},
};
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use substrate_archive_backend::{ApiAccess, BlockExecutor, ReadOnlyBackend as Backend};
use substrate_archive_common::{types::Storage, ReadOnlyDB};
use xtra::prelude::*;

/// The environment passed to each task
pub struct Environment<B, R, C, D>
where
    D: ReadOnlyDB,
    B: BlockT + Unpin,
    B::Hash: Unpin,
{
    backend: Arc<Backend<B, D>>,
    client: Arc<C>,
    storage: Address<StorageAggregator<B>>,
    _marker: PhantomData<R>,
}

type Env<B, R, C, D> = AssertUnwindSafe<Environment<B, R, C, D>>;
impl<B, R, C, D> Environment<B, R, C, D>
where
    D: ReadOnlyDB,
    B: BlockT + Unpin,
    B::Hash: Unpin,
{
    pub fn new(
        backend: Arc<Backend<B, D>>,
        client: Arc<C>,
        storage: Address<StorageAggregator<B>>,
    ) -> Self {
        Self {
            backend,
            client,
            storage,
            _marker: PhantomData,
        }
    }
}

// FIXME:
// we need PhantomData here so that the proc_macro correctly puts PhantomData into the `Job` struct
// + DeserializeOwned so that the types work.
// This is a little bit wonky (and entirely confusing), could be fixed with a better proc-macro in `coil`
// TODO: We should detect when the chain is behind our node, and not execute blocks in this case.
/// Execute a block, and send it to the database actor
#[coil::background_job]
pub fn execute_block<B, RA, Api, D>(
    env: &Env<B, RA, Api, D>,
    block: B,
    _m: PhantomData<(RA, Api, D)>,
) -> Result<(), coil::PerformError>
where
    D: ReadOnlyDB + 'static,
    B: BlockT + DeserializeOwned + Unpin,
    NumberFor<B>: Into<u32>,
    B::Hash: Unpin,
    RA: ConstructRuntimeApi<B, Api> + Send + Sync + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B, D>, B>>,
    Api: ApiAccess<B, Backend<B, D>, RA> + 'static,
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
    let now = std::time::Instant::now();
    let block = BlockExecutor::new(api, &env.backend, block)?.block_into_storage()?;
    log::debug!("Took {:?} to execute block", now.elapsed());
    let storage = Storage::from(block);
    smol::block_on(env.storage.send(storage))?;
    Ok(())
}
