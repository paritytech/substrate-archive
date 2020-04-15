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
use super::{rpc::Rpc, types::{Data, BatchData, Substrate, Block}};

pub fn init<T: Substrate + Send + Sync>(rpc: &Rpc<T>) {
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
                              _: _ => ();
                        }
                    }
                }
            })
    }).expect("Couldn't start a new children group");


    // generate work
    // seperates blocks into different datatypes

   
    // generate work
    // fetches blocks
    let rpc_workers = Bastion::children(|children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone();
                log::info!("Starting work generator");
                let (mut stream, handle) = rpc.subscribe_blocks().unwrap();
                async move {
                    msg!{ctx.recv().await?,
                        msg: Block<T> =!> {
                            process_block(Data::Block(msg));
                            let _ = answer!(ctx, "done");
                        };
                        _: _ => ();
                    } 
                }
                /*    
                    let mut round_robin = 0;
                    while let Some(block) = stream.next().await {
                        let block = stream.next().await;
                        round_robin += 1;
                        round_robin %= workers.elems().len(); 
                        log::info!("Block: {:?}", block);
                        let computed: Answer = ctx.ask(&workers.elems()[round_robin].addr(), block)
                            .map_err(|e| log::error!("{:?}", e)).unwrap();
                        msg! { computed.await?,
                            msg: &str => {
                                println!("Received {}", msg);
                            };
                            _: _ => ();
                        }
                    }
                    // cleanup
                    let res = handle.join();
                    Bastion::stop();
                    Ok(())
                    */
                }
            })
    }).expect("Couldn't start new children group");
    

    Bastion::children(|children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let subscription = rpc.subscribe_finalized_heads().await?;
                loop {
                    log::info!("Awaiting next head...");
                    let head = subscription.next().await;
                    log::info!("Converting to block...");
                    let block = rpc.block(Some(head.hash())).await?;
                    log::info!("Received a new block!");
                    if let Some(b) = block {
                        ctx.ask(rpc_workers.elems()[0].addr(), b)
                            .map_err(|e| log::error!("{:?}", e)).unwrap().await?;
                    } else {
                        log::warn!("Block does not exist");
                    }
                }
                Bastion::stop();
                Ok(())
            })
    })
    
    Bastion::start();
    Bastion::block_until_stopped();
}

pub fn process_block<T: Substrate + Send + Sync>(data: Data<T>) {

    println!("Got Data! {:?}", data);
}

#[allow(dead_code)]
pub fn process_batch_data<T: Substrate + Send + Sync>(data: BatchData<T>) {
    println!("Unused")
}
