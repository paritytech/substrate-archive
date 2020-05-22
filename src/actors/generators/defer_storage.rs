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
    types::{Storage, Substrate, System},
    actors::Broadcast,
    simple_db::SimpleDb
};
use bastion::prelude::*;
use sqlx::PgConnection;
use std::{sync::Arc, hash::{Hash, Hasher}, collections::hash_map::DefaultHasher};

pub fn actor<T>(
    pool: sqlx::Pool<PgConnection>,
    db_workers: ChildrenRef,
    mut storage: Vec<Storage<T>>,
) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    log::info!("Deferring {} storage entries!", storage.len());
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let workers = db_workers.clone();
            let pool = pool.clone(); let mut storage = storage.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                loop {
                    handle_shutdown(&storage, &ctx).await;
                    match entry::<T>(pool.clone(), &mut sched, &mut storage).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                    if !(storage.len() > 0) {break;}
                }
                Ok(())
            }
        })
    })
}

async fn entry<T>(
    pool: sqlx::Pool<PgConnection>,
    sched: &mut Scheduler<'_>,
    storage: &mut Vec<Storage<T>>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>
{
    let mut missing = storage.iter().map(|s| s.block_num()).collect::<Vec<u32>>();
    missing.as_mut_slice().sort();

    let missing = queries::missing_blocks_min_max(&pool, missing[0].into(), missing[missing.len() - 1].into())
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
        let answer = sched
            .ask_next(ready)
            .unwrap()
            .await
            .expect("Couldn't send storage to database");
        log::debug!("{:?}", answer);
    }
    Ok(())
}

async fn handle_shutdown<T>(storage: &Vec<Storage<T>>, ctx: &BastionContext) -> ()
where
    T: Substrate + Send + Sync
{
    if let Some(msg) = ctx.try_recv().await {
        msg! {
            msg,
            ref broadcast: &'static str => {
                log::info!("GOT SHUTDOWN");
                if storage.len() > 0 {
                    log::info!("Storing deferred storage into temporary binary files");
                    let mut hasher = DefaultHasher::new();
                    storage.hash(&mut hasher);

                    let file_name = format!("storage_{:x}", hasher.finish());

                    let mut path = crate::util::substrate_dir();
                    path.push("temp_storage");
                    crate::util::create_dir(path.as_path());
                    path.push(file_name);
                    let temp_db = SimpleDb::new(path).expect("Couldn't create temporary storage files");
                    temp_db.save(storage.clone()).expect("Could not save temp storage");
                }
            };
            e: _ => log::warn!("Received unknown message: {:?}", e);
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;


}
