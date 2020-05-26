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
use crate::actors::{
    scheduler::{Algorithm, Scheduler},
    workers,
};
use crate::{
    backend::{ChainAccess, StorageBackend},
    error::Error as ArchiveError,
    queries,
    types::*,
};
use bastion::prelude::*;
use primitive_types::H256;
use sp_storage::StorageKey;
use sqlx::PgConnection;
use std::sync::Arc;

//TODO need to find a better way to speed up indexing
/// Actor to index storage for PostgreSQL database
/// accepts a list of storage keys to index
/// Indexing all keys will take a very long time. Key prefixes work as well as full keys
pub fn actor<T, C>(
    client: Arc<C>,
    pool: sqlx::Pool<PgConnection>,
    keys: Vec<StorageKey>,
    defer_workers: ChildrenRef,
) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock<T>> + 'static,
    <T as System>::Hash: From<H256>,
    <T as System>::BlockNumber: Into<u32>,
{
    let db = crate::database::Database::new(&pool)?;
    let db_workers = workers::db::<T>(db)?;
    super::collect_storage::actor::<T>(defer_workers.clone())?;
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let db_workers = db_workers.clone();
            let defer_workers = defer_workers.clone();
            let pool = pool.clone();
            let client = client.clone();
            let keys = keys.clone();

            async move {
                let mut max_storage: u32 = 0;
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                sched.add_worker("db", &db_workers);
                sched.add_worker("defer", &defer_workers);
                loop {
                    if handle_shutdown::<T>(&ctx).await {
                        break;
                    }
                    match entry::<T, C>(&client, &pool, &mut sched, &keys, &mut max_storage).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                }
                Ok(())
            }
        })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate storage generator"))
}

async fn entry<T, C>(
    client: &Arc<C>,
    pool: &sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
    keys: &Vec<StorageKey>,
    max_storage: &mut u32,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock<T>> + 'static,
    <T as System>::Hash: From<H256>,
    <T as System>::BlockNumber: Into<u32>,
{
    if queries::are_blocks_empty(pool).await? {
        timer::Delay::new(std::time::Duration::from_secs(5)).await;
        return Ok(());
    }

    let (mut query_from_num, query_from_hash) = queries::get_max_storage(&pool).await.unwrap_or((
        *max_storage,
        client
            .hash(T::BlockNumber::from(*max_storage))?
            .expect("Block doesn't exist!")
            .as_ref()
            .to_vec(),
    ));

    if *max_storage > query_from_num {
        query_from_num = *max_storage;
    } else if query_from_num > *max_storage {
        *max_storage = query_from_num;
    }

    let (mut query_to_num, _) = queries::get_max_block_num(&pool).await?;
    log::info!("Query from num: {}", query_from_num);
    log::info!("Query to num 0: {}", query_to_num);
    if (query_to_num - query_from_num) > 1500 {
        query_to_num = query_from_num + 1000;
    }
    log::info!("Query to num 1: {}", query_to_num);

    *max_storage = query_to_num;
    log::info!("max storage {:?}", *max_storage);

    // we've already collected storage up to most recent block
    // inserting here would cause a Postgres Error (since storage.hash relates to blocks.hash)

    if query_from_num == query_to_num || query_from_num > query_to_num {
        // TODO: block time is 5 seconds so a good choice for sleep?
        // Should we sleep at all?
        timer::Delay::new(std::time::Duration::from_secs(5)).await;
        return Ok(());
    }

    let query_from_hash = H256::from_slice(query_from_hash.as_slice());
    let query_to_hash = client
        .hash(T::BlockNumber::from(query_to_num))?;
    let query_to_hash = if let Some(q) = query_to_hash {
        q
    } else {
        log::warn!("Block does not exist yet!");
        timer::Delay::new(std::time::Duration::from_millis(50)).await;
        return Ok(())
    };

    log::info!(
        "\nquery_from_num={:?}, query_from_hash={:?} \n query_to_num = {:?}, query_to_hash={:?}\n",
        query_from_num,
        hex::encode(query_from_hash.as_bytes()),
        query_to_num,
        hex::encode(query_to_hash.as_ref())
    );

    let storage_backend = StorageBackend::<T, C>::new(client.clone());

    let now = std::time::Instant::now();
    let spawn_keys = keys.clone();
    let change_set = blocking! {
        storage_backend.query_storage(
            T::Hash::from(query_from_hash),
            Some(query_to_hash),
            spawn_keys
        )
    };
    let change_set = spawn!(change_set)
        .await
        .ok_or(ArchiveError::from("Could not spawn blocking"))?
        .unwrap()?;
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

    let now = std::time::Instant::now();
    let storage = change_set
        .into_iter()
        .map(|change| {
            let num = client
                .number(change.block)
                .expect("Could not fetch number for block");
            let block_hash = change.block;
            if let Some(num) = num {
                change
                    .changes
                    .into_iter()
                    .map(|(key, data)| {
                        if num == T::BlockNumber::from(query_from_num) {
                            Storage::new(block_hash, num.into(), true, key, data)
                        } else {
                            Storage::new(block_hash, num.into(), false, key, data)
                        }
                    })
                    .collect::<Vec<Storage<T>>>()
            } else {
                log::warn!("Block doesn't exist!");
                Vec::new()
            }
        })
        .flatten()
        .collect::<Vec<Storage<T>>>();

    let to_defer = storage
        .iter()
        .cloned()
        .filter(|s| missing_blocks.contains(&s.block_num().into()))
        .collect::<Vec<Storage<T>>>();
    
    let storage = storage
        .iter()
        .cloned()
        .filter(|s| !missing_blocks.contains(&s.block_num().into()))
        .collect::<Vec<Storage<T>>>();
    
        let elapsed = now.elapsed();
    log::info!(
        "Took {} seconds, {} milli-seconds to process storage from {} to {}",
        elapsed.as_secs(),
        elapsed.as_millis(),
        query_from_num,
        query_to_num
    );

    if to_defer.len() > 0 {
        log::info!("Storage should be deferred");
        sched.tell_next("defer", to_defer)?;
    }

    if !(storage.len() > 0) {
        return Ok(());
    }

    log::info!("{:?}", storage);
    log::info!("Indexing {} storage entries", storage.len());
    sched.tell_next("db", storage)?;
    Ok(())
}

async fn handle_shutdown<T>(ctx: &BastionContext) -> bool
where
    T: Substrate + Send + Sync,
{
    if let Some(msg) = ctx.try_recv().await {
        msg! {
            msg,
            ref broadcast: super::Broadcast => {
                match broadcast {
                    _ => { return true }
                }
            };
            e: _ => log::warn!("Received unknown message: {:?}", e);
        };
    }
    false
}
