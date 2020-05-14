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

//! Network Actors
//! These aggregate data for child actors to work with
//! they mostly wait on network IO

use crate::{
    rpc::Rpc,
    types::{Block, Substrate, SubstrateBlock},
};
use bastion::prelude::*;
use futures::future::join_all;
use sp_runtime::traits::{Block as _, Header as _};

use super::scheduler::{Algorithm, Scheduler};

const REDUNDANCY: usize = 5;

/// instantiate all the block workers
pub fn actor<T>(decode_workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
{
    let metadata_workers = metadata::<T>(decode_workers, url.clone())?;
    blocks::<T>(metadata_workers, url.clone())
}

/// Subscribe to new blocks via RPC
/// this is a worker that never stops
fn blocks<T>(meta_workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
{
    // actor which produces work in the form of collecting blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let meta_workers = meta_workers.clone();
            let url: String = url.clone();

            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin);
                let rpc = super::connect::<T>(url.as_str()).await;
                let mut subscription = rpc
                    .subscribe_finalized_blocks()
                    .await
                    .expect("Subscription failed");

                loop {
                    log::info!("Awaiting next head...");
                    let head = subscription.next().await;
                    log::info!("Converting to block...");
                    let block = rpc
                        .block(Some(head.hash()))
                        .await
                        .map_err(|e| log::error!("{:?}", e))
                        .unwrap();
                    log::info!("Received a new block!");

                    if let Some(b) = block {
                        log::trace!("{:?}", b);
                        let _ = sched.next(&ctx, &meta_workers, b).unwrap().await?;
                    } else {
                        log::warn!("Block does not exist!");
                    }
                    // TODO: need some kind of handler to break out of the loop
                }
                Ok(())
            }
        })
    })
}

/// fetches metadata about the block or blocks before passing on to decoding
pub fn metadata<T>(workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
{
    Bastion::children(|children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone();
                let url = url.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin);
                    let rpc = Rpc::new(super::connect::<T>(url.as_str()).await);
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: SubstrateBlock<T> =!> {

                                let (ver, meta) = rpc.meta_and_version(Some(block.block.header().hash()).clone()).await.unwrap();
                                let block = Block::<T>::new(block, meta, ver.spec_version);
                                // send block and metadata to decode actors
                                let _ = sched.next(&ctx, &workers, block).unwrap().await?;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Could not answer");
                            };
                            blocks: Vec<SubstrateBlock<T>> =!> {
                                let mut meta_futures = Vec::new();
                                // for first and last block check metadata version
                                // if it's the same, don't get version for rest of blocks
                                // just insert version
                                // you could evolve this to be some kind of sort-algorithm that significantly cuts down
                                // on the amount of RPC calls done
                                let (first, last) = (blocks[0].clone(), blocks[blocks.len()].clone());
                                let first_meta = rpc.meta_and_version(Some(first.block.header().hash()).clone()).await.unwrap();
                                let last_meta = rpc.meta_and_version(Some(last.block.header().hash()).clone()).await.unwrap();

                                let mut batch_items = Vec::new();
                                if first_meta.0.spec_version == last_meta.0.spec_version {
                                    for b in blocks.into_iter() {
                                        batch_items.push(Block::<T>::new(b, first_meta.1.clone(), first_meta.0.spec_version));
                                    }
                                } else {
                                    for b in blocks.iter() {
                                        meta_futures.push(rpc.meta_and_version(Some(b.block.header().hash()).clone()))
                                    }
                                    let metadata = join_all(meta_futures).await.into_iter().collect::<Result<Vec<_>, _>>();

                                    // handle error directly
                                    let metadata = match metadata {
                                        Ok(v) => v,
                                        Err(e) => {
                                            log::error!("{:?}", e);
                                            panic!("Error");
                                        }
                                    };

                                    for (b, m) in blocks.into_iter().zip(metadata.into_iter())  {
                                        batch_items.push(Block::<T>::new(b, m.1, m.0.spec_version));
                                    }
                                }

                                let _ = sched.next(&ctx, &workers, batch_items).unwrap().await?;
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
