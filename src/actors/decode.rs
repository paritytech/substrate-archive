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

use crate::types::{Block, Extrinsic, ExtrinsicType, RawExtrinsic, Substrate};
use bastion::prelude::*;
use desub::{decoder::Decoder, TypeDetective};
use subxt::system::System;

use super::scheduler::{Algorithm, Scheduler};

const REDUNDANCY: usize = 64;

/// the main actor
/// holds the internal decoder state
pub fn actor<T, P>(db_workers: ChildrenRef, decoder: Decoder<P>) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync + 'static,
    <T as System>::BlockNumber: Into<u32>,
{
    // actor that manages decode state, but sends decoding to other actors
    // TODO: could be a supervisor
    // TODO: rework desub so that decoder doesn't need to be cloned everytime we send it to an actor
    // could do a 'stateless' approach that only sends the current spec + metadata to the decoder to decode with
    // rather than keeping all metadata ever presented
    Bastion::children(|children: Children| {
        children
            .with_exec(move |ctx: BastionContext| {
                let workers = db_workers.clone();
                let mut decoder = decoder.clone();
                async move {
                    log::info!("Decode worker started");
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: Block<T> =!> {
                                process_block(block.clone(), &mut sched).await;
                                process_extrinsics::<T, P>(decoder.clone(), vec![block], &mut sched).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("couldn't answer");
                             };
                             blocks: Vec<Block<T>> =!> {
                                 process_blocks(blocks.clone(), &mut sched).await;
                                 process_extrinsics(decoder.clone(), blocks, &mut sched).await;
                                 answer!(ctx, super::ArchiveAnswer::Success).expect("couldn't answer");
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            })
    })
}

pub async fn process_block<T>(block: Block<T>, sched: &mut Scheduler<'_>)
where
    T: Substrate + Send + Sync,
{
    let v = sched.ask_next(block).unwrap().await;
    log::debug!("{:?}", v);
}

pub async fn process_blocks<T>(blocks: Vec<Block<T>>, sched: &mut Scheduler<'_>)
where
    T: Substrate + Send + Sync,
{
    log::info!("Processing blocks");
    let v = sched.ask_next(blocks).unwrap().await;
    log::debug!("{:?}", v);
}

pub async fn process_extrinsics<T, P>(
    mut decoder: Decoder<P>,
    blocks: Vec<Block<T>>,
    sched: &mut Scheduler<'_>,
) where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync + 'static,
    <T as System>::BlockNumber: Into<u32>,
{
    blocks
        .iter()
        .for_each(|b| decoder.register_version(b.spec, &b.meta));

    let extrinsics = blocks
        .iter()
        .map(|b| Vec::<RawExtrinsic<T>>::from(b))
        .flatten()
        .map(|e| {
            let ext = decoder
                .decode_extrinsic(e.spec, e.inner.as_slice())
                .expect("decoding extrinsic failed");
            if ext.is_signed() {
                ExtrinsicType::Signed(Extrinsic::new(ext, e.hash, e.index, e.block_num))
            } else {
                ExtrinsicType::NotSigned(Extrinsic::new(ext, e.hash, e.index, e.block_num))
            }
        })
        .collect::<Vec<ExtrinsicType<T>>>();

    log::info!("Decoded {} extrinsics", extrinsics.len());
    log::debug!("{:?}", extrinsics);
    let v = sched.ask_next(extrinsics).unwrap().await;
    log::debug!("{:?}", v);
}
