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

//! Child worker to storage
//! If a block is missing and a storage entry refers to that block
//! defers inserting storage into the relational database until that block is inserted
//! This actor isn't always running in the background
//! it will be started by the storage actor on a needs basis

use crate::actors::scheduler::{Algorithm, Scheduler};
use crate::{
    error::Error as ArchiveError,
    queries,
    simple_db::SimpleDb,
    types::{Storage, Substrate, System},
};
use bastion::prelude::*;
use sqlx::PgConnection;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

pub fn actor<T>(
    pool: sqlx::Pool<PgConnection>,
) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let db = crate::database::Database::new(&pool).expect("Database intialization error");
    let db_workers = super::db::<T>(db).expect("Could not start storage db workers");
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let workers = db_workers.clone();
            let pool = pool.clone();
            async move {
                let mut storage = Vec::new();
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                sched.add_worker("db", &workers);
                loop {
                    match msg::<T>(&ctx, &mut storage).await {
                        Ok(should_stop) => {
                            if should_stop {
                                break;
                            }
                        },
                        Err(e) => log::error!("{:?}", e)
                    };
                    if storage.len() > 0 {
                        match handle_entries::<T>(pool.clone(), &mut sched, &mut storage).await {
                            Ok(_) => (),
                            Err(e) => log::error!("{:?}", e),
                        }
                    }
                }
                Ok(())
            }
        })
    })
}

/// Handle a message sent to this actor
/// returns true if it should stop
async fn msg<T>(ctx: &BastionContext,
                storage: &mut Vec<Storage<T>>
) -> Result<bool, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>
{
    let msg: Option<SignedMessage>;
    // wait for work if we don't have any (letting actor sleep)
    if storage.len() > 0 {
        msg = ctx.try_recv().await;
    } else {
        msg = Some(ctx.recv().await.expect("Couldn't receive message"));
    }

    if let Some(m) = msg {
        log::info!("Received a message");
        return handle_message(m, ctx, storage).await
    }
    Ok(false)
}

async fn handle_message<T>(msg: SignedMessage, ctx: &BastionContext, storage: &mut Vec<Storage<T>>) -> Result<bool, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>
{
    msg! {
        msg,
        deferred_storage: Vec<Storage<T>> => {
            log::info!("Extending Storage");
            storage.extend(deferred_storage.into_iter());
        };
        ref broadcast: super::Broadcast => {
            log::info!("writing storage to temporary files");
            handle_shutdown(storage, broadcast).await;
        };
        question: super::ArchiveQuestion =!> {
            log::info!("Responding to supervisor");
            match question {
                super::ArchiveQuestion::IsStorageDone => {
                    if storage.len() > 0 {
                        answer!(ctx, super::ArchiveAnswer::StorageNotDone)
                            .expect("Could not answer");
                    } else {
                        answer!(ctx, super::ArchiveAnswer::StorageIsDone)
                            .expect("Could not answer");
                        return Ok(true)
                    }
                }
            }
        };
        e: _ => log::warn!("Received unknown msg {:?}", e);
    }
    Ok(false)
}


async fn handle_entries<T>(
    pool: sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
    storage: &mut Vec<Storage<T>>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let mut missing = storage.iter().map(|s| s.block_num()).collect::<Vec<u32>>();
    missing.as_mut_slice().sort();

    let missing = queries::missing_blocks_min_max(
        &pool,
        missing[0].into(),
        missing[missing.len() - 1].into(),
    )
    .await?
    .into_iter()
    .map(|b| b.generate_series as u32)
    .collect::<Vec<u32>>();

    let mut ready: Vec<Storage<T>> = Vec::new();

    storage.retain(|s| {
        if !missing.contains(&s.block_num().into()) {
            ready.push(s.clone());
            false
        } else {
            true
        }
    });

    if ready.len() > 0 {
        log::info!(
            "STORAGE: inserting {} Deferred storage entries",
            ready.len()
        );
        let answer = sched.tell_next("db", ready)
            .expect("Couldn't send storage to database");
        log::debug!("{:?}", answer);
    } else {
        timer::Delay::new(std::time::Duration::from_millis(100)).await;
    }
    Ok(())
}

async fn handle_shutdown<T>(storage: &mut Vec<Storage<T>>, broadcast: &super::Broadcast) -> ()
where
    T: Substrate + Send + Sync,
{
    match broadcast {
        super::Broadcast::Shutdown => {
            log::info!("GOT SHUTDOWN");
            if storage.len() > 0 {
                let now = std::time::Instant::now();
                log::info!("Storing {} deferred storage entries into temporary binary files", storage.len());
                let mut hasher = DefaultHasher::new();
                storage.hash(&mut hasher);
                let hash = hasher.finish();
                log::info!("hash of storage {:?}", hash);
                let file_name = format!("storage_{:x}", hash);

                let mut path = crate::util::substrate_dir();
                path.push("temp_storage");
                crate::util::create_dir(path.as_path());
                path.push(file_name);
                let temp_db = SimpleDb::new(path).expect("Couldn't create temporary storage files");
                temp_db.save(storage.clone()).expect("Could not save temp storage");
                let elapsed = now.elapsed();
                log::info!(
                    "took {} seconds, {} milli-seconds, {} micro-seconds to save storage",
                    elapsed.as_secs(),
                    elapsed.as_millis(),
                    elapsed.as_micros()
                );
                storage.drain(..);
            }
        },
    }
}
