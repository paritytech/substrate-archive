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

use crate::{
    actors::workers,
    backend::{ApiAccess, BlockChanges, BlockExecutor as BlockExec, ReadOnlyBackend as Backend},
    error::{ArchiveResult, Error as ArchiveError},
    types::*,
};
use hashbrown::HashSet;
use itertools::Itertools;
use sc_client_api::backend;
use sp_api::{ApiExt, ApiRef, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header, NumberFor},
};
use sp_storage::{StorageData, StorageKey as StorageKeyWrapper};
use std::{marker::PhantomData, sync::Arc, thread::JoinHandle};
use xtra::prelude::*;

pub struct BlockExecutor<B, RA, Api>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA>,
{
    pool: rayon::ThreadPool,
    /// Entries that have been inserted (avoids inserting duplicates)
    inserted: HashSet<B::Hash>,
    addr: Address<workers::Aggregator<B>>,
    client: Arc<Api>,
    backend: Arc<Backend<B>>,
    _marker: PhantomData<RA>,
}

impl<B, RA, Api> BlockExecutor<B, RA, Api>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA> + 'static,
{
    /// create a new instance of the executor
    pub fn new(
        num_threads: Option<usize>,
        addr: Address<workers::Aggregator<B>>,
        client: Arc<Api>,
        backend: Arc<Backend<B>>,
    ) -> Result<Self, ArchiveError> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.unwrap_or(0))
            .thread_name(|i| format!("blk-exec-{}", i))
            .build()?;
        let inserted = HashSet::new();
        Ok(Self {
            pool,
            inserted,
            addr,
            client,
            backend,
            _marker: PhantomData,
        })
    }

    fn work(
        block: B,
        client: Arc<Api>,
        backend: Arc<Backend<B>>,
        addr: Address<workers::Aggregator<B>>,
    ) -> Result<(), ArchiveError> {
        let api = client.runtime_api();

        // don't execute genesis block
        if *block.header().parent_hash() == Default::default() {
            return Ok(());
        }

        log::debug!(
            "Executing Block: {}:{}, version {}",
            block.header().hash(),
            block.header().number(),
            client
                .runtime_version_at(&BlockId::Hash(block.header().hash()))?
                .spec_version,
        );

        let changes = BlockExec::new(api, backend.clone(), block)?.block_into_storage()?;
        addr.do_send(changes);
        Ok(())
    }
}

impl<B, RA, Api> Actor for BlockExecutor<B, RA, Api>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA> + 'static,
{
}

pub enum BlockData<Block: BlockT> {
    Batch(Vec<Block>),
    Single(Block),
}

impl<B> Message for BlockData<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    type Result = ArchiveResult<()>;
}

impl<B, RA, Api> SyncHandler<BlockData<B>> for BlockExecutor<B, RA, Api>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA> + 'static,
{
    fn handle(&mut self, data: BlockData<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        match data {
            BlockData::Batch(v) => {
                let to_insert = v
                    .into_iter()
                    .filter(|b| !self.inserted.contains(&b.hash()))
                    .collect::<Vec<_>>();
                if to_insert.len() > 0 {
                    // we try to execute at least 5 blocks at once, this lets rayon
                    // avoid looking for work too much and using up CPU time
                    for blocks in to_insert.chunks(5) {
                        self.inserted.extend(blocks.iter().map(|b| b.hash()));
                        let blocks = blocks.to_vec();
                        let client = self.client.clone();
                        let backend = self.backend.clone();
                        let addr = self.addr.clone();
                        self.pool.spawn_fifo(move || {
                            for block in blocks.into_iter() {
                                match Self::work(
                                    block,
                                    client.clone(),
                                    backend.clone(),
                                    addr.clone(),
                                ) {
                                    Ok(_) => (),
                                    Err(e) => log::error!("{:?}", e),
                                }
                            }
                        });
                    }
                }
            }
            BlockData::Single(b) => {
                let client = self.client.clone();
                let backend = self.backend.clone();
                let addr = self.addr.clone();

                self.pool
                    .spawn_fifo(move || match Self::work(b, client, backend, addr) {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    });
            }
        }
        Ok(())
    }
}
