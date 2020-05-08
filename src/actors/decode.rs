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

//! Actors which do work by decoding data before it's inserted into the database
//! these actors may do highly parallelized work
//! These actors do not make any external connections to a Database or Network

use crate::types::{Block, ChainInfo as _, Extrinsic, Substrate};
use bastion::prelude::*;
use desub::{decoder::Decoder, TypeDetective};

use super::scheduler::{Scheduler, Algorithm};

const REDUNDANCY: usize = 64;

/// the main actor
/// holds the internal decoder state
pub fn actor<T, P>(decoder: Decoder<P>) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync + 'static,
{
    let workers = Bastion::children(|children: Children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                async move {
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: Block<T> =!> { // should be Storage<T> 
                                process_block(block);
                                let _ = answer!(ctx, super::ArchiveAnswer::Success);
                            };
                            blocks: Vec<Block<T>> =!> {
                                log::info!("Got {} blocks", blocks.len());
                                let _ = answer!(ctx, super::ArchiveAnswer::Success).expect("Could not answer");
                            };
                            extrinsics: (Decoder<P>, Vec<Extrinsic<T>>) =!> {
                                log::info!("Got Extrinsic!");
                                let (decoder, extrinsics) = extrinsics;
                                let mut ext = Vec::new();
                                for e in extrinsics.iter() {
                                    ext.push(
                                        decoder.decode_extrinsic(e.spec, e.inner.as_slice()).expect("Decoding extrinsic failed")
                                    )
                                }
                                log::info!("Decoded {} extrinsics", ext.len());
                                log::debug!("{:?}", ext);
                                let _ = answer!(ctx, super::ArchiveAnswer::Success).expect("could not answer");
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            }) 
    }).expect("Could not start worker actor");

    // top-level actor
    // actor that manages decode state, but sends decoding to other actors
    // TODO: could be a supervisor
    // TODO: rework desub so that decoder doesn't need to be cloend everytime we send it to an actor
    // could do a 'stateless' approach that only sends the current spec + metadata to the decoder to decode with
    // rather than keeping all metadata ever presented
    Bastion::children(|children: Children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let workers = workers.clone();
                let mut decoder = decoder.clone();
                async move {
                    log::info!("Decode worker started");
                    let mut sched = Scheduler::new(Algorithm::RoundRobin);
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: Block<T> =!> {

                                decoder.register_version(block.spec, &block.meta);
                                let _ = sched.next(&ctx, &workers, block.clone())
                                    .map_err(|e| log::error!("{:?}", e)).unwrap().await?;

                                let extrinsics: Vec<Extrinsic<T>> = (&block).into();
                                let answer = sched.next(&ctx, &workers, (decoder.clone(), extrinsics))
                                    .map_err(|e| log::error!("{:?}", e)).expect("Failed to send extrinsics to actor").await?;
                                answer!(ctx, answer).expect("couldn't answer");
                             };
                             blocks: Vec<Block<T>> =!> {
                                 blocks.iter().for_each(|b| decoder.register_version(b.spec, &b.meta));

                                 let answer = sched.next(&ctx, &workers, blocks)
                                     .map_err(|e| log::error!("{:?}", e)).unwrap().await?;
                                answer!(ctx, answer).expect("couldn't answer");
                                
                            };
                             e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            })
    })
}

pub fn process_block<T: Substrate + Send + Sync>(block: Block<T>) {
    println!("Got Block {}, version: {}", block.get_hash(), block.spec)
}
