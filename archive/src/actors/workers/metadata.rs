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

use crate::actors::scheduler::{Algorithm, Scheduler};
use crate::{
    error::Error as ArchiveError,
    queries,
    rpc::Rpc,
    types::{Block, DbPool, Metadata, Substrate, SubstrateBlock, System},
};
use bastion::prelude::*;
use serde::de::DeserializeOwned;
use sp_runtime::traits::{Block as _, Header as _};

const REDUNDANCY: usize = 5;

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
pub fn actor<T>(url: String, pool: DbPool) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Header: DeserializeOwned,
{
    let transform_workers = super::transformers::actor::<T>(pool.clone())?;
    Bastion::children(|children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = transform_workers.clone();
                let url = url.clone();
                let pool = pool.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                    sched.add_worker("transform", &workers);
                    match handle_msg::<T>(&mut sched, &pool, url.as_str()).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                    Ok(())
                }
            })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate metadata workers"))
}

async fn handle_msg<T>(
    sched: &mut Scheduler<'_>,
    pool: &DbPool,
    url: &str,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Header: DeserializeOwned,
{
    let rpc = super::connect::<T>(url).await;
    loop {
        msg! {
            sched.context().recv().await.expect("Could not receive"),
            block: SubstrateBlock<T> => {
                meta_process_block::<T>(block, rpc.clone(), &pool, sched).await?;
            };
            blocks: Vec<SubstrateBlock<T>> =!> {
                meta_process_blocks(blocks, rpc.clone(), &pool, sched).await?;
                crate::archive_answer!(sched.context(), super::ArchiveAnswer::Success)?;
            };
            ref _broadcast: super::Broadcast => {
                ()
            };
            e: _ => log::warn!("Received unknown data {:?}", e);
        }
    }
}

async fn meta_process_block<T>(
    block: SubstrateBlock<T>,
    rpc: Rpc<T>,
    pool: &DbPool,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    let hash = block.block.header().hash();
    let ver = rpc.version(Some(hash).as_ref()).await?;
    meta_checker(ver.spec_version, Some(hash), &rpc, pool, sched).await?;
    let block = Block::<T>::new(block, ver.spec_version);
    let v = sched.ask_next("transform", block)?.await;
    log::debug!("{:?}", v);
    Ok(())
}

async fn meta_process_blocks<T>(
    blocks: Vec<SubstrateBlock<T>>,
    rpc: Rpc<T>,
    pool: &DbPool,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    log::info!("Got {} blocks", blocks.len());
    let mut batch_items = Vec::new();

    let now = std::time::Instant::now();
    let first = rpc.version(Some(&blocks[0].block.header().hash())).await?;
    let elapsed = now.elapsed();
    log::debug!(
        "Rpc request for version took {} milli-seconds",
        elapsed.as_millis()
    );
    let last = rpc
        .version(Some(blocks[blocks.len() - 1].block.header().hash()).as_ref())
        .await?;
    log::info!(
        "First Version: {}, Last Version: {}",
        first.spec_version,
        last.spec_version
    );
    // if first and last versions of metadata are the same, we only need to do one check
    if first == last {
        meta_checker(
            first.spec_version,
            Some(blocks[0].block.header().hash()),
            &rpc,
            pool,
            sched,
        )
        .await?;
        blocks
            .into_iter()
            .for_each(|b| batch_items.push(Block::<T>::new(b, first.spec_version)));
    } else {
        for b in blocks.into_iter() {
            let hash = b.block.header().hash();
            let ver = rpc.version(Some(hash).as_ref()).await?;
            meta_checker(ver.spec_version, Some(hash), &rpc, pool, sched).await?;
            batch_items.push(Block::<T>::new(b, ver.spec_version))
        }
    }

    let v = sched.ask_next("transform", batch_items)?.await;
    log::debug!("{:?}", v);
    Ok(())
}

// checks if the metadata exists in the database
// if it doesn't exist yet, fetch metadata and insert it
async fn meta_checker<T>(
    ver: u32,
    hash: Option<T::Hash>,
    rpc: &Rpc<T>,
    pool: &DbPool,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    if !queries::check_if_meta_exists(ver, pool).await? {
        let meta = rpc.metadata(hash).await?;
        let meta = Metadata::new(ver, meta);
        let v = sched.ask_next("transform", meta)?.await;
        log::debug!("{:?}", v);
    }
    Ok(())
}
