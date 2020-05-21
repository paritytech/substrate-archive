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

//! Indexes storage

use crate::{backend::{ChainAccess, StorageBackend}, error::Error as ArchiveError, queries, types::*};
use crate::actors::{self, scheduler::{Algorithm, Scheduler}, workers};
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
    let db_workers = workers::db::<T>(db).expect("Could not start storage db workers");
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let workers = db_workers.clone();
            let pool = pool.clone();
            let client = client.clone();
            let keys = keys.clone();
            async move {
                let mut max_storage: u32 = 0;
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                loop {
                    match entry::<T, C>(&client, &pool, &mut sched, &keys, &mut max_storage).await {
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
    max_storage: &mut u32
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::Hash: From<H256>,
{
    if queries::is_blocks_empty(pool).await? {
        async_std::task::sleep(Duration::from_secs(5)).await;
        return Ok(());
    }

    let (query_from_num, query_from_hash) = queries::get_max_storage(&pool).await.unwrap_or((
        *max_storage,
        client
            .hash(*max_storage)?
            .expect("Block doesn't exist!")
            .as_ref()
            .to_vec(),
    ));

    let (mut query_to_num, _) = queries::get_max_block_num(&pool).await?;

    if (query_to_num - query_from_num) > 1500 {
         query_to_num = query_from_num + 500;
    }

    // we've already collected storage up to most recent block
    // inserting here would cause a Postgres Error (since storage.hash relates to blocks.hash)
    if query_from_num == query_to_num || query_from_num > query_to_num {
        // TODO: block time is 5 seconds so a good choice for sleep?
        async_std::task::sleep(Duration::from_secs(5)).await;
        return Ok(());
    }

    let query_from_hash = H256::from_slice(query_from_hash.as_slice());
    let query_to_hash = client.hash(query_to_num)?.expect("Block not found");

    log::info!(
        "\nquery_from_num={:?}, query_from_hash={:?} \n query_to_num = {:?}, query_to_hash={:?}\n",
        query_from_num,
        hex::encode(query_from_hash.as_bytes()),
        query_to_num,
        hex::encode(query_to_hash.as_bytes())
    );

    let storage_backend = StorageBackend::<T, C>::new(client.clone());

    let now = std::time::Instant::now();
    let change_set = storage_backend.query_storage(T::Hash::from(query_from_hash), Some(T::Hash::from(query_to_hash)), keys.clone())?;
    let elapsed = now.elapsed();   
    log::info!(
        "Took {} seconds, {} milli-seconds to query storage from {} to {}",
        elapsed.as_secs(),
        elapsed.as_millis(),
        query_from_num,
        query_to_num
    );

    let missing_blocks = queries::missing_blocks_min_max(&pool, query_from_num, query_to_num)
        .await?
        .into_iter()
        .map(|b| b.generate_series as u32)
        .collect::<Vec<u32>>();

    let storage = change_set.into_iter().map(|change| {
        let num = client.number(H256::from_slice(change.block.as_ref())).expect("Couldn't get block number for hash");
        let block_hash = change.block;
        if let Some(num) = num {
            change
                .changes
                .into_iter()
                .map(|(key, data)| {
                    if num == query_from_num {
                        Storage::new(T::Hash::from(block_hash), num, true, key, data)
                    } else {
                        Storage::new(T::Hash::from(block_hash), num, false, key, data)
                    }
                }).collect::<Vec<Storage<T>>>()
        } else {
            log::warn!("Block doesn't exist!");
            Vec::new()
        }
    }).flatten().collect::<Vec<Storage<T>>>();

    let to_defer = storage.iter().cloned().filter(|s| missing_blocks.contains(&s.block_num())).collect::<Vec<Storage<T>>>();
    let storage = storage.iter().cloned().filter(|s| !missing_blocks.contains(&s.block_num())).collect::<Vec<Storage<T>>>();
    *max_storage = query_to_num;

    log::info!("MAX STORAGE {:?}", *max_storage);

    if to_defer.len() > 0 {
        super::defer_storage::actor::<T>(pool.clone(), sched.workers().clone(), to_defer)
            .expect("Couldn't start defer workers");
    }

    if !(storage.len() > 0) {
        return Ok(());
    }

    log::info!("{:?}", storage);
    log::info!("Indexing {} storage entries", storage.len());
    let answer = sched
        .ask_next(storage)
        .unwrap()
        .await
        .expect("Couldn't send storage to database");
    log::debug!("{:?}", answer);
    Ok(())
}
