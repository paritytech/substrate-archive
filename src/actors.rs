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
use async_ctrlc::CtrlC;
use futures::future::FutureExt;
use std::{env, sync::Arc};

// TODO: 'cut!' macro to handle errors from within actors

/// initialize substrate archive
/// Requires a substrate client, url to running RPC node, and a list of keys to index from storage
/// EX: If you want to query all keys for 'System Account'
/// twox('System') + twox('Account')
/// Prefixes are preferred, they will be more performant
pub async fn init<T, C>(client: Arc<C>, url: String, keys: Vec<StorageKey>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock<T>> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Hash: From<primitive_types::H256>,
    <T as System>::Header: serde::de::DeserializeOwned,
{
    Bastion::init();

    // TODO: could be initialized asyncronously somewhere
    let pool = run!(
        PgPool::builder()
            .max_size(10)
            .build(&env::var("DATABASE_URL")?)
    )?;

    // Normally workers aren't started on the top-level
    // however we want access to this in order to send messages on program shutdown
    let defer_workers = workers::defer_storage::<T>(pool.clone())
        .expect("Couldn't start defer workers");
    let storage = self::generators::storage::<T, _>(client.clone(), pool.clone(), keys, defer_workers.clone())
        .expect("Couldn't add storage indexer");

    // network generator. Gets headers from network but uses client to fetch block bodies
    let network = self::generators::network::<T, _>(client.clone(), pool.clone(), url.clone())
        .expect("Couldn't add blocks child");

    // IO/kvdb generator (missing blocks). Queries the database to get missing blocks
    // uses client to get those blocks
    let missing = self::generators::db::<T, _>(client, pool, url).expect("Couldn't start db work generators");

    Bastion::start();

    let ctrlc = CtrlC::new().expect("Couldn't create ctrl-c handler");
    ctrlc.then(|_| async {
        Bastion::broadcast(Broadcast::Shutdown).expect("Couldn't send message");
        network.stop().expect("Network Generator could not be stopped");
        storage.stop().expect("Storage could not be stopped");
        missing.stop().expect("Missing Blocks Worker could not be stopped");
        finish_work(&defer_workers).await;
        Bastion::kill();
    }).await;

    Bastion::block_until_stopped();
    Ok(())
}

async fn finish_work(defer_workers: &ChildrenRef) {
    loop {
        let mut answers_needed = defer_workers.elems().len();
        let mut answers = Vec::new();
        for worker in defer_workers.elems() {
            let ans = worker.ask_anonymously(ArchiveQuestion::IsStorageDone)
                            .expect("Couldn't send shutdown message to defer storage");
            answers.push(ans);
        }
        let answers = futures::future::join_all(answers).await;
        for answer in answers.into_iter() {
            msg! {
                answer.expect("Could not receive answer"),
                msg: ArchiveAnswer => {
                    match msg {
                        ArchiveAnswer::StorageIsDone => {
                            answers_needed -= 1;
                        },
                        ArchiveAnswer::StorageNotDone => {
                            timer::Delay::new(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                        e @ _ => log::warn!("Unexpected Answer {:?}", e)
                    };
                };
                e: _ => log::warn!("Unexpected message {:?}", e);
            }
        }
        if answers_needed == 0 {
            break;
        } else {
            timer::Delay::new(std::time::Duration::from_millis(10)).await;
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArchiveQuestion {
    /// as storage actors if the storage is finished being deferred
    IsStorageDone
}

#[derive(Debug, PartialEq, Clone)]
pub enum ArchiveAnswer {
    Success,
    /// Storage actor response that Storage is Done
    StorageIsDone,
    /// Storage actor response that storage is not finished saving progress
    StorageNotDone,

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
