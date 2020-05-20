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
use crate::{backend::ChainAccess, error::Error as ArchiveError, queries, types::*};
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
    log::info!(
        "storage num: {:?}, storage hash {:?}",
        max_storage_num,
        storage_hash
    );
    log::info!("block num: {:?}, block hash {:?}", max_block, block_hash);
    let storage_hash = H256::from_slice(storage_hash.as_slice());
    let max_block_hash = H256::from_slice(block_hash.as_slice());

    if (max_block - max_storage_num) > 1500 {
        max_block = max_storage_num + 1500;
    }

    let storage = Mutex::new(Vec::new());
    log::info!("Indexing storage from {} to {}", max_storage_num, max_block);
    for num in (max_storage_num..max_block) {
        keys.par_iter().for_each(|key| {
            let now = std::time::Instant::now();
            let storage_pairs = client.storage_pairs(&BlockId::Number(num), key).unwrap();
            let elapsed = now.elapsed();
            log::info!(
                "Took {} seconds for storage pairs, {} milli-seconds",
                elapsed.as_secs(),
                elapsed.as_millis()
            );
            let hash = client.hash(num).unwrap().unwrap(); // TODO: Handle None
            let s = storage_pairs
                .into_iter()
                .map(|(k, v)| Storage::new(T::Hash::from(hash), num, k, v))
                .collect::<Vec<Storage<T>>>();
            (*storage.lock().unwrap()).extend(s.into_iter());
        })
    }
    let storage = storage.into_inner()?;
    log::info!("Indexing {} storage entries", storage.len());
    let answer = sched
        .ask_next(storage)
        .unwrap()
        .await
        .expect("Couldn't send storage to transformers");
    log::debug!("{:?}", answer);
    Ok(())
}
