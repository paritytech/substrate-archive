// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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

//! Executes blocks concurrently

use crate::{
    backend::{ApiAccess, BlockChanges, BlockExecutor, ReadOnlyBackend as Backend},
    error::{ArchiveResult, Error as ArchiveError},
    types::{self, PriorityIdent, ThreadPool},
};
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header, NumberFor},
};
use std::{marker::PhantomData, sync::Arc};

// pub type StorageKey = Vec<u8>;
// pub type StorageValue = Vec<u8>;
// pub type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;
// pub type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

#[derive(Debug)]
pub enum BlockData<B: BlockT> {
    Batch(Vec<types::Block<B>>),
    Single(types::Block<B>),
}

/// Executor that sends blocks to a thread pool for execution
pub struct BlockExecPool<Block: BlockT, RA, Api> {
    /// the threadpool
    pool: rayon::ThreadPool,
    client: Arc<Api>,
    backend: Arc<Backend<Block>>,
    _marker: PhantomData<(Block, RA)>,
}

impl<B, RA, Api> BlockExecPool<B, RA, Api>
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
        client: Arc<Api>,
        backend: Arc<Backend<B>>,
    ) -> Result<Self, ArchiveError> {
        // channel pair for sending and receiving BlockChanges

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.unwrap_or(0))
            .thread_name(|i| format!("blk-exec-{}", i))
            .build()?;

        Ok(Self {
            pool,
            client,
            backend,
            _marker: PhantomData,
        })
    }

    fn work(
        block: B,
        client: &Arc<Api>,
        backend: &Arc<Backend<B>>,
        sender: &flume::Sender<BlockChanges<B>>,
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

        let block = BlockExecutor::new(api, backend, block)?.block_into_storage()?;

        sender.send(block).expect("Could not send");
        Ok(())
    }

    /// inserts tasks for the threadpool
    /// returns the number of tasks that were inserted
    pub fn add_vec_task(
        &self,
        blocks: Vec<types::Block<B>>,
        sender: flume::Sender<BlockChanges<B>>,
    ) -> Result<usize, ArchiveError> {
        let len = blocks.len();

        for blocks in blocks.chunks(5) {
            let client = self.client.clone();
            let backend = self.backend.clone();
            let sender = sender.clone();
            let blocks = blocks.to_vec();
            self.pool.spawn_fifo(move || {
                for block in blocks.into_iter() {
                    match Self::work(block.inner.block, &client, &backend, &sender) {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                }
            });
        }
        Ok(len)
    }
}

impl<B, R, A> ThreadPool for BlockExecPool<B, R, A>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    R: ConstructRuntimeApi<B, A> + Send + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    A: ApiAccess<B, Backend<B>, R> + 'static,
{
    type In = types::Block<B>;
    type Out = BlockChanges<B>;

    fn add_task(
        &self,
        d: Vec<types::Block<B>>,
        tx: flume::Sender<BlockChanges<B>>,
    ) -> ArchiveResult<usize> {
        self.add_vec_task(d, tx)
    }
}

impl<B: BlockT> PriorityIdent for types::Block<B> {
    type Ident = u32;
    fn identifier(&self) -> u32 {
        self.spec
    }
}
