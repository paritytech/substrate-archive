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
    queries,
    rpc::Rpc,
    types::{Block, Metadata, Substrate, SubstrateBlock},
};
use bastion::prelude::*;
use futures::future::join_all;
use sp_runtime::traits::{Block as _, Header as _};
use sqlx::PgConnection;

use super::scheduler::{Algorithm, Scheduler};

const REDUNDANCY: usize = 10;

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
pub fn actor<T>(
    transform_workers: ChildrenRef,
    url: String,
    pool: sqlx::Pool<PgConnection>,
) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
{
    Bastion::children(|children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = transform_workers.clone();
                let url = url.clone();
                let pool = pool.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                    let rpc = Rpc::new(super::connect::<T>(url.as_str()).await);
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: SubstrateBlock<T> =!> {
                                meta_process_block::<T>(block, rpc.clone(), &pool, &mut sched).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Could not answer");
                            };
                            blocks: Vec<SubstrateBlock<T>> =!> {
                                meta_process_blocks(blocks, rpc.clone(), &pool, &mut sched).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Could not answer");
                                // send batch_items to decode actor
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            })
    })
}

async fn meta_process_block<T>(
    block: SubstrateBlock<T>,
    rpc: Rpc<T>,
    pool: &sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
) where
    T: Substrate + Send + Sync,
{
    let hash = block.block.header().hash();
    let ver = rpc.version(Some(hash).clone()).await.unwrap();
    meta_checker(ver.spec_version, Some(hash), &rpc, pool, sched).await;
    let block = Block::<T>::new(block, ver.spec_version);
    let v = sched.ask_next(block).unwrap().await;
    log::debug!("{:?}", v);
}

async fn meta_process_blocks<T>(
    blocks: Vec<SubstrateBlock<T>>,
    rpc: Rpc<T>,
    pool: &sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
) where
    T: Substrate + Send + Sync,
{
    log::info!("Got {} blocks", blocks.len());
    let mut batch_items = Vec::new();

    let now = std::time::Instant::now();
    let first = rpc
        .version(Some(blocks[0].block.header().hash()))
        .await
        .unwrap();
    let elapsed = now.elapsed();
    log::info!(
        "Rpc request for version took {} milli-seconds",
        elapsed.as_millis()
    );
    let last = rpc
        .version(Some(blocks[blocks.len() - 1].block.header().hash()))
        .await
        .unwrap();
    log::info!(
        "First Version: {}, Last Version: {}",
        first.spec_version,
        last.spec_version
    );
    if first == last {
        meta_checker(
            first.spec_version,
            Some(blocks[0].block.header().hash()),
            &rpc,
            pool,
            sched,
        )
        .await;
        blocks
            .into_iter()
            .for_each(|b| batch_items.push(Block::<T>::new(b, first.spec_version)));
    } else {
        for b in blocks.into_iter() {
            let hash = b.block.header().hash();
            let ver = rpc.version(Some(hash).clone()).await.unwrap();
            meta_checker(ver.spec_version, Some(hash), &rpc, pool, sched).await;
            batch_items.push(Block::<T>::new(b, ver.spec_version))
        }
    }

    let v = sched.ask_next(batch_items).unwrap().await;
    log::debug!("{:?}", v);
}
struct SplitBlocks {
    block: u32,
    spec: u32,
}

async fn meta_checker<T>(
    ver: u32,
    hash: Option<T::Hash>,
    rpc: &Rpc<T>,
    pool: &sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
) where
    T: Substrate + Send + Sync,
{
    if !queries::check_if_meta_exists(ver, pool)
        .await
        .expect("Couldn't check if meta version exists")
    {
        let meta = rpc.metadata(hash).await.expect("Couldn't get metadata");
        let meta = Metadata::new(ver, meta);
        let v = sched.ask_next(meta).unwrap().await;
    }
}
