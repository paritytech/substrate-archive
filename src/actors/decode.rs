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

use crate::{types::{Block, Substrate, Data}, error::Error as ArchiveError};
use desub::{
    decoder::{Decoder, Metadata},
    TypeDetective
};
use bastion::prelude::*;

const REDUNDANCY: usize = 5;

/// the main actor
/// holds the internal decoder state
pub fn actor<T>() -> Result<ChildrenRef, ()>
where
    T: Substrate
{
    Bastion::children(|children: Children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                async move {
                    log::info!("Decode worker started");
                    loop {
                        msg! {
                            ctx.recv().await?,
                            msg: Block<T> =!> { // should be Storage<T> 
                                block(Data::Block(msg));
                                let _ = answer!(ctx, "done");
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            })
    })
}

pub fn block<T: Substrate + Send + Sync>(data: Data<T>) {
    println!("Got Data! {:?}", data);
}

pub fn decode_extrinsics() {
    unimplemented!()
}

pub fn decode_storage() {
    unimplemented!()
}

pub fn decode_events() {
    unimplemented!() 
}

pub fn decode_constants() {
    unimplemented!() 
}