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
mod db_generators;
mod decode;
mod network;
mod scheduler;

use super::{
    backend::ChainAccess,
    database::Database,
    error::Error as ArchiveError,
    types::{NotSignedBlock, Substrate},
};
use bastion::prelude::*;
use sqlx::postgres::PgPool;
use std::{env, sync::Arc};
use subxt::system::System;

use desub::{decoder::Decoder, TypeDetective};

// TODO: 'cut!' macro to handle errors from within actors

/// initialize substrate archive
/// if a child actor panics or errors, it is up to the supervisor to handle it
pub fn init<T, P, C>(decoder: Decoder<P>, client: Arc<C>, url: String) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    P: TypeDetective + Send + Sync + 'static,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::BlockNumber: Into<u32>,
{
    Bastion::init();

    /// TODO: could be initialized asyncronously somewhere
    let pool = async_std::task::block_on(
        PgPool::builder()
            .max_size(10)
            .build(&env::var("DATABASE_URL")?),
    )?;

    let db = Database::new(&pool)?;

    // TODO use answers to handle errors in the supervisor
    // maybe add a custom configured supervisor later
    // but the defaults seem to be working fine so far...
    let db_workers = self::database::actor::<T>(db).expect("Couldn't start database workers");
    let decode_workers =
        self::decode::actor::<T, P>(db_workers, decoder).expect("Couldn't start decode children");
    self::network::actor::<T>(decode_workers.clone(), url).expect("Couldn't add blocks child");
    self::db_generators::actor::<T, _>(client, pool).expect("Couldn't start db work generators");

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
