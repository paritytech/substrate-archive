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
    backend::{ReadOnlyBackend, RuntimeVersionCache},
    error::{ArchiveResult, Error},
    queries,
    types::{BatchBlock, Block},
};
use futures::{Stream, StreamExt};
use rayon::prelude::*;
use sp_runtime::{
    generic::SignedBlock,
    traits::{Block as BlockT, Header as _, NumberFor},
};
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
    rt_cache: RuntimeVersionCache<B>,
    /// the last maximum block number from which we are sure every block before then is indexed
    last_max: u32,
}

impl<B: BlockT + Unpin> BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
{
    pub fn new(
        backend: Arc<ReadOnlyBackend<B>>,
        addr: DatabaseAct<B>,
        ag: Address<Aggregator<B>>,
    ) -> Self {
        Self {
            rt_cache: RuntimeVersionCache::new(backend.clone()),
            last_max: 0,
            backend,
            db: addr,
            ag,
        }
    }

    /// A async wrapper around the backend fn `iter_blocks` which
    /// runs in a `spawn_blocking` async task (it's own thread)
    async fn collect_blocks(
        &self,
        fun: impl Fn(u32) -> bool + Send + 'static,
    ) -> ArchiveResult<Vec<Block<B>>> {
        let backend = self.backend.clone();
        let now = std::time::Instant::now();
        let gather_blocks = move || -> ArchiveResult<Vec<SignedBlock<B>>> {
            Ok(backend
                .iter_blocks(|n| fun(n))?
                .enumerate()
                .inspect(|(i, b)| {
                    if i % 50_000 == 0 {
                        log::info!("Last block loaded {}", b.block.header().number());
                    }
                })
                .map(|(_, b)| b)
                .collect())
        };
        let blocks = smol::unblock!(gather_blocks())?;
        log::info!("Took {:?} to get blocks", now.elapsed());
        let now = std::time::Instant::now();
        let cache = self.rt_cache.clone();
        let versions = smol::unblock!(cache.find_versions(blocks.as_slice()))?;
        let elapsed = now.elapsed();
        for ver in versions.iter() {
            println!("Start: {}, End: {}", ver.start, ver.end);
        }
        println!("Took {:?} seconds to get all versions", elapsed);
        panic!("Versions found");
    }

    /// First run of indexing
    /// gets any blocks that are missing from database and indexes those
    /// sets the `last_max` value.
    async fn re_index(&mut self) -> ArchiveResult<()> {
        let mut conn = self.db.send(GetState::Conn.into()).await?.await?.conn();
        let numbers = queries::missing_blocks_min_max(&mut conn, self.last_max).await?;
        let len = numbers.len();
        log::info!("{} missing blocks", len);

        let mut max: u32 = self.last_max;
        let now = std::time::Instant::now();
        let mut stream = self.collect_blocks(move |n| numbers.contains(&n)).await?;
        panic!("Done");
        /*
        while let Some(block) = stream.next().await {
            let block = block?;
            if (*block.inner.block.header().number()).into() > max {
                max = (*block.inner.block.header().number()).into();
            }
            self.ag.send(block).await?;
        }
        */
        log::info!("Took {:#?} to crawl {} blocks", now.elapsed(), len);

        if max == 0 {
            self.last_max = queries::max_block(&mut conn).await?;
        } else {
            self.last_max = max;
        }
        log::info!("New max: {}", self.last_max);
        Ok(())
    }

    async fn crawl(&mut self) -> ArchiveResult<()> {
        let copied_last_max = self.last_max;
        let mut stream = self.collect_blocks(move |n| n > copied_last_max).await?;
        let mut max: u32 = self.last_max;
        /*
        for block in stream.next().await.unwrap() {
            if (*block.inner.block.header().number()).into() > max {
                max = (*block.inner.block.header().number()).into();
            }
            self.ag.send(block).await?;
        }
        */
        self.last_max = max;
        log::info!("New max: {}", self.last_max);
        Ok(())
    }
}

impl<B: BlockT> Actor for BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
    B: Unpin,
{
    fn started(&mut self, ctx: &mut Context<Self>) {
        let this = ctx.address().expect("Actor just started");
        // using this instead of notify_immediately because
        // ReIndexing is async process
        this.do_send(ReIndex)
            .expect("Actor cannot be disconnected; just started");
        // ctx.notify_interval(std::time::Duration::from_secs(5), || Crawl);
    }
}

struct Crawl;
impl Message for Crawl {
    type Result = ();
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
            Ok(_) => (),
        }
        /*
                match self.crawl().await {
                    Err(e) => log::error!("{}", e.to_string()),
                    Ok(b) => {
                        if let Err(_) = self.ag.do_send(BatchBlock::new(b)) {
                            ctx.stop();
                        }
                    }
                }
        */
    }
}

struct ReIndex;
impl Message for ReIndex {
    type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<ReIndex> for BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, _: ReIndex, ctx: &mut Context<Self>) {
        log::info!("Beginning to index blocks..");
        match self.re_index().await {
            Err(e) => log::error!("{}", e.to_string()),
            Ok(_) => (),
        }
        /*
        match self.re_index().await {
            Err(e) => log::error!("{}", e.to_string()),
            Ok(b) => {
                if let Err(_) = self.ag.do_send(BatchBlock::new(b)) {
                    ctx.stop();
                }
            }
        }
        */
    }
}
