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

use super::{
    actor_pool::ActorPool,
    workers::{Aggregator, DatabaseActor, GetState},
};
use crate::{
    backend::{GetRuntimeVersion, ReadOnlyBackend},
    error::ArchiveResult,
    queries,
    threadpools::BlockData,
    types::{BatchBlock, Block},
};
use futures::Stream;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use std::sync::Arc;
use xtra::prelude::*;

type DatabaseAct<B> = Address<ActorPool<DatabaseActor<B>>>;

pub struct BlocksIndexer<B: BlockT>
where
    NumberFor<B>: Into<u32>,
{
    /// background task to crawl blocks
    backend: Arc<ReadOnlyBackend<B>>,
    db: DatabaseAct<B>,
    ag: Address<Aggregator<B>>,
    api: Arc<dyn GetRuntimeVersion<B>>,
    last_max: u32,
}

impl<B: BlockT> BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
{
    pub fn new(
        backend: Arc<ReadOnlyBackend<B>>,
        addr: DatabaseAct<B>,
        ag: Address<Aggregator<B>>,
        api: Arc<dyn GetRuntimeVersion<B>>,
    ) -> Self {
        Self {
            backend,
            db: addr,
            ag,
            api,
            last_max: 0,
        }
    }

    async fn crawl(&mut self) -> ArchiveResult<Vec<Block<B>>> {
        let mut conn = self.db.send(GetState::Conn.into()).await?.await?.conn();
        let numbers = queries::missing_blocks_min_max(&mut conn, self.last_max).await?;
        let now = std::time::Instant::now();
        let copied_last_max = self.last_max;
        // rocksdb iteration is a blocking IO task
        let blocks = collect_blocks(self.backend.clone(), self.api.clone(), move |n| {
            numbers.contains(&n) || n > copied_last_max
        })
        .await?;
        log::info!("Took {:#?} to crawl", now.elapsed());
        self.last_max = blocks
            .iter()
            .map(|b| (*b.inner.block.header().number()).into())
            .fold(0, |ac, e| if e > ac { e } else { ac });
        Ok(blocks)
    }
}

struct Crawl;
impl Message for Crawl {
    type Result = ();
}

impl<B: BlockT> Actor for BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
    B: Unpin,
{
    fn started(&mut self, ctx: &mut Context<Self>) {
        ctx.notify_interval(std::time::Duration::from_secs(5), || Crawl);
    }
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<Crawl> for BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, _: Crawl, ctx: &mut Context<Self>) {
        log::info!("Crawling for blocks");
        match self.crawl().await {
            Err(e) => log::error!("{}", e.to_string()),
            Ok(b) => {
                if let Err(_) = self.ag.do_send(BatchBlock::new(b)) {
                    ctx.stop();
                }
            }
        }
    }
}

/// A wrapper around the backend fn `iter_blocks` which
/// runs in a `spawn_blocking` async task (it's own thread)
async fn collect_blocks<B: BlockT>(
    backend: Arc<ReadOnlyBackend<B>>,
    api: Arc<dyn GetRuntimeVersion<B>>,
    fun: impl Fn(u32) -> bool + Send + 'static,
) -> ArchiveResult<impl Stream<Item = Block<B>>> {
    let gather_blocks = move || -> ArchiveResult<Box<dyn Iterator<Item = Block<B>>>> {
        log::info!("HELLO FROM GATHER BLOCKS");
        Ok(Box::new(backend.iter_blocks(|n| fun(n))?.map(|b| {
            let ver = api.runtime_version(&BlockId::Number(*b.block.header().number()))?;
            Ok(Block::new(b, ver.spec_version))
        })))
    };
    log::info!("Gathering blocks");
    smol::unblock!(gather_blocks())
}
