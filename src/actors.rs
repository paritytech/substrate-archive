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

//! where the main actor framework is defined

mod network;
mod decode;
mod database;

use bastion::prelude::*;
use futures::{StreamExt, Stream, channel::mpsc};
use std::sync::Arc;
use runtime_primitives::traits::Header as _;
use super::{rpc::Rpc, types::{Data, BatchData, Substrate, Block}, error::Error as ArchiveError};



pub fn init<T: Substrate>(url: String) -> Result<(), ArchiveError> {
    Bastion::init();

    // maybe add a custom configured supervisor later
    // but the defaults seem to be working fine so far...

    let decode_workers = self::decode::actor::<T>().expect("Couldn't start decode children");
    self::network::blocks::<T>(decode_workers.clone(), url).expect("Couldn't add blocks child");

    // generate work
    // seperates blocks into different datatypes

   
    // generate work
    // fetches blocks
    
    
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
pub fn process_batch<T: Substrate + Send + Sync>(data: BatchData<T>) {
    println!("Unused")
}
