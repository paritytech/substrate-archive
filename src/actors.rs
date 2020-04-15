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

use bastion::prelude::*;
use futures::{StreamExt, Stream, channel::mpsc};
use std::sync::Arc;
use runtime_primitives::traits::Header as _;
use super::{rpc::Rpc, types::{Data, BatchData, Substrate, Block}, error::Error as ArchiveError};

pub fn init<T: Substrate + Send + Sync>(url: String) -> Result<(), ArchiveError> {
    Bastion::init();
    
    let workers = Bastion::children(|children: Children| {
        children
            .with_redundancy(5)
            .with_exec(move |ctx: BastionContext| {
                async move {
                    log::info!("Worker Started");

                    loop {
                        msg! {ctx.recv().await?,
                              msg: Block<T> =!> {
                                  process_block(Data::Block(msg));
                                  let _ = answer!(ctx, "done");
                              };
                              e: _ => log::warn!("received unknown data: {:?}", e);
                        }
                    }
                }
            })
    }).expect("Couldn't start a new children group");


    // generate work
    // seperates blocks into different datatypes

   
    // generate work
    // fetches blocks
    
    // actor which produces work 
    Bastion::children(|children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone(); 
                let url = url.clone(); 
                
                async move {
                    let mut round_robin = 0;
                    let rpc = connect::<T>(url.as_str()).await;
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
    }).expect("Couldn't start new children group");
    
    Bastion::start();
    Bastion::block_until_stopped();
    Ok(())
}

/// connect to the substrate RPC
/// each actor may potentially have their own RPC connections
async fn connect<T: Substrate + Send + Sync>(url: &str) -> subxt::Client<T> {
    subxt::ClientBuilder::<T>::new()
        .set_url(url)
        .build().await.map_err(|e| log::error!("{:?}", e)).unwrap()
}

pub fn process_block<T: Substrate + Send + Sync>(data: Data<T>) {

    println!("Got Data! {:?}", data);
}

#[allow(dead_code)]
pub fn process_batch_data<T: Substrate + Send + Sync>(data: BatchData<T>) {
    println!("Unused")
}
