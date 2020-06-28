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

//! sync actor to prevent blocking the main executor

use crate::types::*;
use crate::{
    actors::{generators::msg, workers},
    backend::{BlockBroker, BlockData, GetRuntimeVersion, ReadOnlyBackend},
    error::ArchiveResult,
};
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header as _, NumberFor},
};
use std::sync::Arc;
use xtra::prelude::*;

pub struct BlockFetcher<Block: BlockT> {
    pool: rayon::ThreadPool,
    broker: BlockBroker<Block>,
    backend: Arc<ReadOnlyBackend<Block>>,
    rt_fetch: Arc<dyn GetRuntimeVersion<Block>>,
    addr: Address<workers::Metadata>,
}

impl<Block: BlockT> BlockFetcher<Block> {
    /// create a new BlockFetcher
    /// Must be ran within the context of a executor
    pub fn new(
        url: &str,
        pool: sqlx::PgPool,
        backend: Arc<ReadOnlyBackend<Block>>,
        broker: BlockBroker<Block>,
        rt_fetch: Arc<dyn GetRuntimeVersion<Block>>,
        num_threads: Option<usize>,
    ) -> ArchiveResult<Self> {
        let addr = workers::Metadata::new(url.to_string(), pool).spawn();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.unwrap_or(0))
            .thread_name(|i| format!("blk-fetch-{}", i))
            .build()?;
        Ok(Self {
            addr,
            backend,
            pool,
            broker,
            rt_fetch,
        })
    }
}

impl<Block: BlockT> Actor for BlockFetcher<Block> {}

pub struct BlockRange(pub Vec<u32>);
impl Message for BlockRange {
    type Result = ArchiveResult<()>;
}

// should probably make this a real threadpool with `block_worker` num threads
// the reason we dont split up work here is because we don't want to block the `blocks` actor
// from inserting the most-recent blocks into the database
impl<B: BlockT> SyncHandler<BlockRange> for BlockFetcher<B> {
    fn handle(&mut self, block_nums: BlockRange, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        for block_num in block_nums.0.into_iter() {
            let backend = self.backend.clone();
            let broker = self.broker.clone();
            let addr = self.addr.clone();
            let rt_fetch = self.rt_fetch.clone();

            self.pool.spawn_fifo(move || {
                let num = NumberFor::<B>::from(block_num);
                let b = backend.block(&BlockId::Number(num));
                if b.is_none() {
                    log::warn!("Block {} not found!", block_num);
                } else {
                    let b = b.expect("Checked for none; qed");
                    broker.work.send(BlockData::Single(b.block.clone())).unwrap();
                    // TODO: fix unwrap
                    let version = rt_fetch.runtime_version(&BlockId::Hash(b.block.hash())).unwrap();
                    let block = Block::<B>::new(b, version.spec_version);
                    addr.do_send(block).expect("Actor disconnected");
                }
            });
        }
        Ok(())
    }
}

impl<B: BlockT> SyncHandler<msg::Head<B>> for BlockFetcher<B> {
    fn handle(&mut self, head: msg::Head<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let head = head.0;
        let backend = self.backend.clone();
        let broker = self.broker.clone();
        let addr = self.addr.clone();
        let rt_fetch = self.rt_fetch.clone();
        self.pool.spawn_fifo(move || {
            let block = backend.block(&BlockId::Number(*head.number()));
            if block.is_none() {
                log::warn!("Block {} not found!", head.number());
                return;
            }
            let block = block.expect("Checked for none; qed");
            broker.work.send(BlockData::Single(block.block.clone())).unwrap();
            let version = rt_fetch
                .runtime_version(&BlockId::Hash(block.block.hash()))
                .unwrap();
            let block = Block::<B>::new(block, version.spec_version);
            addr.do_send(block).expect("Actor Disconnected");
        });
        Ok(())
    }
}
