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

use bastion::prelude::*;
use runtime_primitives::traits::{Header as _, Block as _};
use futures::future::join_all;
use crate::{error::Error as ArchiveError, types::{Block, Substrate, BatchBlockItem, SubstrateBlock}, rpc::Rpc};

const REDUNDANCY: usize = 5;

// will be used when `batch_blocks` is coded
// just instantiates all the actors
pub fn actor(workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()> {
    unimplemented!()
}

pub fn blocks<T>(workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()> 
where
    T: Substrate,
{
    // actor which produces work in the form of collecting blocks
    Bastion::children(|children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone(); 
                let url: String = url.clone(); 
                
                async move {
                    let mut round_robin: usize = 0;
                    let rpc = super::connect::<T>(url.as_str()).await;
                    let mut subscription = rpc.subscribe_finalized_blocks().await.expect("Subscription failed");
                        // .map_err(|e| log::error!("{:?}", e)).unwrap();
                    while let block = subscription.next().await {
                       
                        log::info!("Awaiting next head...");
                        let head = subscription.next().await;
                        log::info!("Converting to block...");
                        let block = rpc.block(Some(head.hash())).await.map_err(|e| log::error!("{:?}", e)).unwrap();
                        log::info!("Received a new block!");
                        
                        if let Some(b) = block {
                            log::trace!("{:?}", b);
                            round_robin += 1;
                            round_robin %= workers.elems().len();
                            ctx.ask(&workers.elems()[round_robin].addr(), Block::<T>::new(b))
                                .map_err(|e| log::error!("{:?}", e)).unwrap().await?;
                        } else {
                            log::warn!("Block does not exist!");
                        }
                    }
                    Bastion::stop();
                    Ok(())
                }
            })
    })
}

pub fn batch_blocks() -> Result<ChildrenRef, ()> {
    unimplemented!()
}

/// fetches metadata about the block or blocks before passing on to decoding
pub fn metadata<T>(workers: ChildrenRef, url: String) -> Result<ChildrenRef, ()> 
where
    T: Substrate + Send + Sync
{
     
    Bastion::children(|children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone();
                let url = url.clone();
                async move {
                    let mut round_robin: usize = 0;
                    let rpc = Rpc::new(super::connect::<T>(url.as_str()).await);
                    
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: Block<T> =!> {
                                    let metadata = rpc.meta_and_version(Some(block.inner().block.header().hash()).clone()).await;
                                    // send block and metadata to decode actors
                            };
                            blocks: Vec<SubstrateBlock<T>> =!> {
                                let mut meta_futures = Vec::new();
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
                                
                                let mut batch_items = Vec::new();
                                for (b, m) in blocks.into_iter().zip(metadata.into_iter())  {
                                    batch_items.push(BatchBlockItem::<T>::new(b, m.1, m.0.spec_version));
                                }
                                // send batch_items to decode actor
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            })
    })
}

