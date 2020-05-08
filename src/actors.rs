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

mod database;
mod decode;
mod network;
mod scheduler;
mod db_generators;

use super::{
    error::Error as ArchiveError,
    types::Substrate,
};
use bastion::prelude::*;

use desub::{decoder::Decoder, TypeDetective};

// TODO: 'cut!' macro to handle errors from within actors

/// initialize substrate archive
/// if a child actor panics or errors, it is up to the supervisor to handle it
pub fn init<T, P>(decoder: Decoder<P>, url: String) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync + 'static,
{
    Bastion::init();

    // TODO use answers to handle errors in the supervisor
    // maybe add a custom configured supervisor later
    // but the defaults seem to be working fine so far...

    //let decode_workers = self::decode::actor::<T, P>(decoder).expect("Couldn't start decode children");
    let decode_workers =
        self::decode::actor::<T, P>(decoder).expect("Couldn't start decode children");
    self::network::actor::<T>(decode_workers.clone(), url).expect("Couldn't add blocks child");

    // generate work
    // seperates blocks into different datatypes

    // generate work
    // fetches blocks

    Bastion::start();
    Bastion::block_until_stopped();
    Ok(())
}

#[derive(Debug)]
pub enum ArchiveAnswer {
    Success,
    Fail(ArchiveError),
}

/// connect to the substrate RPC
/// each actor may potentially have their own RPC connections
async fn connect<T: Substrate + Send + Sync>(url: &str) -> subxt::Client<T> {
    subxt::ClientBuilder::<T>::new()
        .set_url(url)
        .build()
        .await
        .map_err(|e| log::error!("{:?}", e))
        .unwrap()
}
