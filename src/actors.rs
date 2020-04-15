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
use super::{rpc::Rpc, types::{Data, BatchData, Substrate}};

pub fn init<T: Substrate + Send + Sync>(rpc: Arc<Rpc<T>>) {
    Bastion::init();
    let workers = Bastion::children(|children: Children| {
        children
            .with_redundancy(10)
            .with_exec(move |ctx: BastionContext| {
                async move {
                    log::info!("Worker Started");

                    loop {
                        msg! {ctx.recv().await?,
                              msg: Data<T> =!> {
                                  process_data(msg);
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
    Bastion::children(|children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone();
                let rpc = rpc.clone();
                log::info!("Starting work generator");
                async move {
                    let (mut stream, handle) = rpc.subscribe_blocks().unwrap();
                    for block in stream.next().await {
                         log::info!("Block: {:?}", block);
                         for id_worker_pair in workers.elems().iter().enumerate() {
                            let computed: Answer = ctx.ask(&id_worker_pair.1.addr(), block.clone())
                                .map_err(|e| log::error!("{:?}", e)).unwrap();
                            msg! { computed.await?,
                                  msg: &str => {
                                      println!("Source received {}", msg);
                                  };
                                  _: _ => ();
                            }
                        }
                    }
                    let res = handle.join();
                    Bastion::stop();
                    Ok(())
                }
            })
    }).expect("Couldn't start new children group");
    
    Bastion::start();
    Bastion::block_until_stopped();
}

pub fn process_data<T: Substrate + Send + Sync>(data: Data<T>) {
    println!("Got Data! {:?}", data);
}

#[allow(dead_code)]
pub fn process_batch_data<T: Substrate + Send + Sync>(data: BatchData<T>) {
    println!("Unused")
}
