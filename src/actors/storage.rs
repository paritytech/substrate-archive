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

//! Indexes storage deltas

use super::scheduler::{Algorithm, Scheduler};
use crate::{backend::{ChainAccess, StorageBackend}, error::Error as ArchiveError, queries, types::*};
use bastion::prelude::*;
use primitive_types::H256;
use rayon::prelude::*;
use sp_blockchain::HeaderBackend;
use sp_runtime::generic::BlockId;
use sp_storage::{StorageChangeSet, StorageData, StorageKey};
use sqlx::PgConnection;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use subxt::system::System;

//TODO need to find a better way to speed up indexing
/// Actor to index storage for PostgreSQL database
/// accepts a list of storage keys to index
/// Indexing all keys will take a very long time. Key prefixes work as well as full keys
pub fn actor<T, C>(
    client: Arc<C>,
    pool: sqlx::Pool<PgConnection>,
    keys: Vec<StorageKey>,
) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::Hash: From<H256>,
    <T as System>::BlockNumber: Into<u32>,
{
    let db = crate::database::Database::new(&pool).expect("Database intialization error");
    let db_workers = super::database::actor::<T>(db).expect("Couldn't start storage db workers");

    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let workers = db_workers.clone();
            let pool = pool.clone();
            let client = client.clone();
            let keys = keys.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                loop {
                    match entry::<T, C>(&client, &pool, &mut sched, &keys).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                }
                Ok(())
            }
        })
    })
}

pub async fn entry<T, C>(
    client: &Arc<C>,
    pool: &sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
    keys: &Vec<StorageKey>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::Hash: From<H256>,
{
    if queries::is_storage_empty(pool).await? {
        async_std::task::sleep(Duration::from_secs(5)).await;
        return Ok(());
    }

    let (max_storage_num, storage_hash) = queries::get_max_storage(&pool).await.unwrap_or((
        0,
        client
            .hash(0)
            .expect("Couldn't get hash")
            .unwrap()
            .as_ref()
            .to_vec(),
    ));

    let (mut max_block, block_hash) = queries::get_max_block_num(&pool).await?;

    // we've already collected storage up to most recent block
    if max_storage_num == max_block {
        // sleep for 5 secs (block time)
        async_std::task::sleep(Duration::from_secs(5)).await;
        return Ok(());
    }

    let storage_backend = StorageBackend::<T, C>::new(client.clone());

    log::info!(
        "storage num: {:?}, storage hash {:?}",
        max_storage_num,
        storage_hash
    );
    log::info!("block num: {:?}, block hash {:?}", max_block, block_hash);
    let storage_hash = H256::from_slice(storage_hash.as_slice());
    let mut max_block_hash = H256::from_slice(block_hash.as_slice());

    if (max_block - max_storage_num) > 1500 {
        max_block = max_storage_num + 100;
        max_block_hash = client.hash(max_block)?.expect("Block not found!");
    }

    log::info!("Indexing storage from {} to {}", max_storage_num, max_block);
    let now = std::time::Instant::now();
    let change_set = storage_backend.query_storage(T::Hash::from(storage_hash), Some(T::Hash::from(max_block_hash)), keys.clone())?;
    let elapsed = now.elapsed();
    log::info!(
        "Took {} seconds for query storage, {} milli-seconds",
        elapsed.as_secs(),
        elapsed.as_millis()
    );
    let storage = change_set.into_iter().map(|change| {
        let num = client.number(H256::from_slice(change.block.as_ref())).expect("Couldn't get block number for hash");
        let block_hash = change.block;
        if let Some(num) = num {
            change
                .changes
                .into_iter()
                .filter_map(|(k, d)| {
                    if d.is_some() {
                        Some((k, d))
                    } else {
                        None
                    }
                })
                .map(|c| {
                    Storage::new(T::Hash::from(block_hash), num, c.0, c.1.expect("checked in filter"))
                }).collect::<Vec<Storage<T>>>()
        } else {
            log::error!("Block doesn't exist!");
            Vec::new()
        }
    }).flatten().collect::<Vec<Storage<T>>>();

    log::info!("Indexing {} storage entries", storage.len());
    let answer = sched
        .ask_next(storage)
        .unwrap()
        .await
        .expect("Couldn't send storage to transformers");
    log::debug!("{:?}", answer);
    Ok(())
}
