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
mod storage;
mod metadata;
mod network;
mod scheduler;
mod transformers;

use super::{
    backend::ChainAccess,
    database::Database,
    error::Error as ArchiveError,
    types::{NotSignedBlock, Substrate},
};
use sp_storage::StorageKey;
use bastion::prelude::*;
use sqlx::postgres::PgPool;
use std::{env, sync::Arc};
use subxt::system::System;

// TODO: 'cut!' macro to handle errors from within actors

/// initialize substrate archive
/// Requires a substrate client, url to running RPC node, and a list of keys to index from storage
/// EX: If you want to query all keys for 'System Account'
/// twox('System') + twox('Account')
/// Prefixes are preferred, they will be more performant
pub fn init<T, C>(client: Arc<C>, url: String, keys: Vec<StorageKey>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Hash: From<primitive_types::H256>
{
    Bastion::init();

    /// TODO: could be initialized asyncronously somewhere
    let pool = async_std::task::block_on(
        PgPool::builder()
            .max_size(15)
            .build(&env::var("DATABASE_URL")?),
    )?;

    let db = Database::new(&pool)?;

    // TODO use answers to handle errors in the supervisor
    // maybe add a custom configured supervisor later
    // but the defaults seem to be working fine so far...
    let db_workers = self::database::actor::<T>(db).expect("Couldn't start database workers");
    let transformers = self::transformers::actor::<T, _>(db_workers.clone(), client.clone())
        .expect("Couldn't start transform workers");
    let meta_workers = self::metadata::actor::<T>(transformers.clone(), url.clone(), pool.clone())
        .expect("Couldnt start metadata");

   
    self::storage::actor::<T, _>(client.clone(), pool.clone(), keys)
        .expect("Couldn't add storage indexer");

    // network generator. Gets headers from network but uses client to fetch block bodies
    self::network::actor::<T, _>(meta_workers.clone(), client.clone(), url)
        .expect("Couldn't add blocks child");

    // IO/kvdb generator (missing blocks)
    self::db_generators::actor::<T, _>(client, meta_workers.clone(), pool)
        .expect("Couldn't start db work generators");

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
