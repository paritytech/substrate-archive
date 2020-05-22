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

mod generators;
mod scheduler;
mod workers;

use super::{
    backend::ChainAccess,
    error::Error as ArchiveError,
    types::{NotSignedBlock, Substrate, System},
};
use bastion::prelude::*;
use sp_storage::StorageKey;
use sqlx::postgres::PgPool;
use std::{env, sync::Arc};

// TODO: 'cut!' macro to handle errors from within actors

/// initialize substrate archive
/// Requires a substrate client, url to running RPC node, and a list of keys to index from storage
/// EX: If you want to query all keys for 'System Account'
/// twox('System') + twox('Account')
/// Prefixes are preferred, they will be more performant
pub fn init<T, C>(client: Arc<C>, url: String, keys: Vec<StorageKey>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock<T>> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Hash: From<primitive_types::H256>,
    <T as System>::Header: serde::de::DeserializeOwned,
{
    Bastion::init();

    ctrlc::set_handler(move || {
        // so far, broadcast is the only thing using a &'static str type
        Bastion::broadcast(Broadcast::Shutdown).expect("Couldn't send message");
        // give two seconds for the system to shutdown
        // FIXME: should really have a better system then "ehh...  it shouldn't take more than a second"
        std::thread::sleep(std::time::Duration::from_secs(1));
        Bastion::stop();
    })
    .expect("Error setting Ctrl-C handler");

    // TODO: could be initialized asyncronously somewhere
    let pool = async_std::task::block_on(
        PgPool::builder()
            .max_size(15)
            .build(&env::var("DATABASE_URL")?),
    )?;

    self::generators::storage::<T, _>(client.clone(), pool.clone(), keys)
        .expect("Couldn't add storage indexer");

    // network generator. Gets headers from network but uses client to fetch block bodies
    self::generators::network::<T, _>(client.clone(), pool.clone(), url.clone())
        .expect("Couldn't add blocks child");

    // IO/kvdb generator (missing blocks). Queries the database to get missing blocks
    // uses client to get those blocks
    self::generators::db::<T, _>(client, pool, url).expect("Couldn't start db work generators");

    Bastion::start();
    Bastion::block_until_stopped();
    Ok(())
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArchiveAnswer {
    Success,
}

/// Messages that are sent to every actor if something happens that must be handled globally
/// like a CTRL-C signal
#[derive(Debug, PartialEq, Clone)]
pub enum Broadcast {
    /// We need to shutdown for one reason or the other
    Shutdown,
}

/// connect to the substrate RPC
/// each actor may potentially have their own RPC connections
async fn connect<T: Substrate + Send + Sync>(url: &str) -> crate::rpc::Rpc<T> {
    crate::rpc::Rpc::connect(url)
        .await
        .expect("Couldn't connect to rpc")
}
