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
    rpc::Rpc,
    types::{Block, Substrate, SubstrateBlock},
};
use bastion::prelude::*;
use futures::future::join_all;
use sp_runtime::traits::{Block as _, Header as _};

use super::scheduler::{Algorithm, Scheduler};

const REDUNDANCY: usize = 5;

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
pub fn actor<T>(decode_workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
{
    Bastion::children(|children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = decode_workers.clone();
                let url = url.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                    let rpc = Rpc::new(super::connect::<T>(url.as_str()).await);
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: SubstrateBlock<T> =!> {
                                meta_process_block::<T>(block, rpc.clone(), &mut sched).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Could not answer");
                            };
                            blocks: Vec<SubstrateBlock<T>> =!> {
                                meta_process_blocks(blocks, rpc.clone(), &mut sched).await;
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

async fn meta_process_block<T>(block: SubstrateBlock<T>, rpc: Rpc<T>, sched: &mut Scheduler<'_>)
where
    T: Substrate + Send + Sync,
{
    let (ver, meta) = rpc
        .meta_and_version(Some(block.block.header().hash()).clone())
        .await
        .unwrap();
    let block = Block::<T>::new(block, meta, ver.spec_version);
    // send block and metadata to decode actors
    let v = sched.ask_next(block).unwrap().await;
    log::debug!("{:?}", v);
}

async fn meta_process_blocks<T>(
    blocks: Vec<SubstrateBlock<T>>,
    rpc: Rpc<T>,
    sched: &mut Scheduler<'_>,
) where
    T: Substrate + Send + Sync,
{
    let mut meta_futures = Vec::new();
    // for first and last block check metadata version
    // if it's the same, don't get version for rest of blocks
    // just insert version
    // you could evolve this to be some kind of sort-algorithm that significantly cuts down
    // on the amount of RPC calls done
    let (first, last) = (blocks[0].clone(), blocks[blocks.len()].clone());

    let first_meta = rpc
        .meta_and_version(Some(first.block.header().hash()).clone())
        .await
        .unwrap();

    let last_meta = rpc
        .meta_and_version(Some(last.block.header().hash()).clone())
        .await
        .unwrap();

    let mut batch_items = Vec::new();
    if first_meta.0.spec_version == last_meta.0.spec_version {
        for b in blocks.into_iter() {
            batch_items.push(Block::<T>::new(
                b,
                first_meta.1.clone(),
                first_meta.0.spec_version,
            ));
        }
    } else {
        for b in blocks.iter() {
            meta_futures.push(rpc.meta_and_version(Some(b.block.header().hash()).clone()))
        }
        let metadata = join_all(meta_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>();

        // handle error directly
        let metadata = match metadata {
            Ok(v) => v,
            Err(e) => {
                log::error!("{:?}", e);
                panic!("Error");
            }
        };

        for (b, m) in blocks.into_iter().zip(metadata.into_iter()) {
            batch_items.push(Block::<T>::new(b, m.1, m.0.spec_version));
        }
    }

    let v = sched.ask_next(batch_items).unwrap().await;
    log::debug!("{:?}", v);
}
