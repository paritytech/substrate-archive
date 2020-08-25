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

use crate::types::*;
use crate::{
    actors::ActorContext,
    backend::{GetRuntimeVersion, ReadOnlyBackend},
    error::ArchiveResult,
};
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, NumberFor},
};
use std::sync::Arc;

pub struct ThreadedBlockFetcher<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    pool: rayon::ThreadPool,
    backend: Arc<ReadOnlyBackend<B>>,
    api: Arc<dyn GetRuntimeVersion<B>>,
}

impl<B> ThreadedBlockFetcher<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    pub fn new(context: ActorContext<B>, num_threads: Option<usize>) -> ArchiveResult<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.unwrap_or(0))
            .thread_name(|i| format!("blk-fetch-{}", i))
            .build()?;
        let api = context.api();

        Ok(Self {
            pool,
            api,
            backend: context.backend().clone(),
        })
    }

    /// Represents one unit of work for the threadpool
    fn work(
        block_num: u32,
        api: &Arc<dyn GetRuntimeVersion<B>>,
        backend: &Arc<ReadOnlyBackend<B>>,
        tx: &flume::Sender<Block<B>>,
    ) -> ArchiveResult<()> {
        let num = NumberFor::<B>::from(block_num);
        let b = backend.block(&BlockId::Number(num));
        if b.is_none() {
            log::warn!("Block {} not found!", num);
        } else {
            let b = b.expect("Checked for none; qed");
            let version = api.runtime_version(&BlockId::Hash(b.block.hash()))?;
            tx.send(Block::<B>::new(b, version.spec_version))?;
        }
        Ok(())
    }

    fn add_task(&self, nums: &[u32], sender: flume::Sender<Block<B>>) -> ArchiveResult<usize> {
        for nums in nums.chunks(10) {
            let api = self.api.clone();
            let backend = self.backend.clone();
            let tx = sender.clone();
            let nums = nums.to_vec();
            self.pool.spawn_fifo(move || {
                for num in nums.into_iter() {
                    match Self::work(num, &api, &backend, &tx) {
                        Ok(_) => (),
                        Err(e) => log::error!("{}", e.to_string()),
                    }
                }
            });
        }
        Ok(nums.len())
    }
}

impl<B> ThreadPool for ThreadedBlockFetcher<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    type In = u32;
    type Out = Block<B>;

    fn add_task(&self, d: Vec<u32>, tx: flume::Sender<Block<B>>) -> ArchiveResult<usize> {
        self.add_task(&d, tx)
    }
}

impl PriorityIdent for u32 {
    type Ident = u32;
    fn identifier(&self) -> u32 {
        *self
    }
}
