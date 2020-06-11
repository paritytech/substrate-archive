// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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

use crate::actors::{
    scheduler::{Algorithm, Scheduler},
    workers,
};
use crate::{
    backend::{ApiAccess, BlockChanges, BlockExecutor},
    error::Error as ArchiveError,
    types::{Block, NotSignedBlock, Storage, Substrate, System},
};
use bastion::prelude::*;
use sc_client_api::backend;
use sc_client_db::Backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use std::sync::Arc;

pub const WORKER_POOL: &str = "worker_pool";
pub const WORKER_DB: &str = "db";
pub const WORKER_COUNT: usize = 2;
pub const BLOCKS_PER_BATCH: usize = 256;

type BlkChanges<T> = BlockChanges<NotSignedBlock<T>>;

pub fn actor<T, Runtime, ClientApi>(
    client: Arc<ClientApi>,
    backend: Arc<Backend<NotSignedBlock<T>>>,
    pool: sqlx::PgPool,
) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    // weird how this trait works
    Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error>
        + ApiExt<
            NotSignedBlock<T>,
            StateBackend = backend::StateBackendFor<Backend<NotSignedBlock<T>>, NotSignedBlock<T>>,
        >,
    ClientApi: ApiAccess<NotSignedBlock<T>, Backend<NotSignedBlock<T>>, Runtime> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::BlockNumber: From<u32>,
{
    // let db = crate::database::Database::new(&pool)?;
    // let db_workers = workers::db::<T>(db)?;
    let child_workers =
        child_actor::<T, Runtime, ClientApi>(client.clone(), backend.clone(), pool)?;
    Bastion::children(|children: Children| {
        children
            .with_redundancy(1)
            .with_exec(move |ctx: BastionContext| {
                let child_workers = child_workers.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                    sched.add_worker(WORKER_POOL, &child_workers);
                    match entry::<T>(&mut sched).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    };
                    Ok(())
                }
            })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate full storage workers"))
}

/// Pool of workers that process blocks
fn child_actor<T, Runtime, ClientApi>(
    client: Arc<ClientApi>,
    backend: Arc<Backend<NotSignedBlock<T>>>,
    pool: sqlx::PgPool,
) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    // weird how this trait works
    Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error>
        + ApiExt<
            NotSignedBlock<T>,
            StateBackend = backend::StateBackendFor<Backend<NotSignedBlock<T>>, NotSignedBlock<T>>,
        >,
    ClientApi: ApiAccess<NotSignedBlock<T>, Backend<NotSignedBlock<T>>, Runtime> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::BlockNumber: From<u32>,
{
    let db = crate::database::Database::new(&pool)?;
    let db_workers = workers::db::<T>(db)?;

    Bastion::children(|children: Children| {
        children
            .with_redundancy(WORKER_COUNT)
            .with_exec(move |ctx: BastionContext| {
                let client = client.clone();
                let backend = backend.clone();
                let db_workers = db_workers.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                    sched.add_worker(WORKER_DB, &db_workers);
                    match child_entry::<T, Runtime, _>(&ctx, &client, &backend, &mut sched).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    };
                    Ok(())
                }
            })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate full storage workers"))
}

/// gathers blocks in a queue and sends them to be executed by worker pool 32 blocks at a time
async fn entry<T>(sched: &mut Scheduler<'_>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    log::info!("Max of {} block workers", WORKER_COUNT);
    let mut block_queue: Vec<Block<T>> = Vec::new();
    'main: loop {
        msg! {
            sched.context().recv().await.expect("Could Not Receive"),
            block: Block<T> => {
                block_queue.push(block);
            };
            blocks: Vec<Block<T>> => {
                log::info!("Got {} blocks", blocks.len());
                block_queue.extend(blocks);
            };
            ref broadcast: super::Broadcast => {
                match broadcast {
                    _ => break 'main,
                }
            };
            e: _ => log::warn!("Received unknown data {:?}", e);
        }
        try_execute_batch(&mut block_queue, sched)?;
    }
    Ok(())
}

fn try_execute_batch<T>(
    blocks: &mut Vec<Block<T>>,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    let mut len = blocks.len();
    let blocks_per_worker = blocks.len() / WORKER_COUNT;

    if len > BLOCKS_PER_BATCH {
        for _ in 0..WORKER_COUNT {
            let blocks = blocks
                .drain(0..blocks_per_worker)
                .collect::<Vec<Block<T>>>();
            sched.tell_next("worker_pool", blocks)?;
            log::info!("Sending {} blocks to be executed", blocks_per_worker);
            len -= blocks_per_worker;
        }
        Ok(())
    } else {
        Ok(())
    }
}

async fn child_entry<T, Runtime, ClientApi>(
    ctx: &BastionContext,
    client: &Arc<ClientApi>,
    backend: &Arc<Backend<NotSignedBlock<T>>>,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi>,
    Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error>
        + ApiExt<
            NotSignedBlock<T>,
            StateBackend = backend::StateBackendFor<Backend<NotSignedBlock<T>>, NotSignedBlock<T>>,
        >,
    ClientApi: ApiAccess<NotSignedBlock<T>, Backend<NotSignedBlock<T>>, Runtime> + 'static,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::BlockNumber: From<u32>,
{
    'main: loop {
        msg! {
            ctx.recv().await.expect("Could Not Receive"),
            blocks: Vec<Block<T>> => {
                let mut futures = Vec::new();
                let len = blocks.len();
                for block in blocks.into_iter() {
                    let client = client.clone();
                    let backend = backend.clone();
                    /* let future = blocking!((move || {
                        let runtime_api = client.runtime_api();
                        BlockExecutor::new(runtime_api, backend.clone(), block.inner.block)
                            .unwrap()
                            .block_into_storage()
                            .unwrap()
                    })());*/

                    futures.push(async move { Vec::new() });
                }
                let now = std::time::Instant::now();
                let changes: Vec<Storage<T>> = futures::future::join_all(futures).await.into_iter().flat_map(|c| {
                    Vec::<Storage<T>>::from(c/* .unwrap() */)
                }).collect();
                let elapsed = now.elapsed();
                log::info!("Took {} seconds, {} milli-seconds to execute {} blocks", elapsed.as_secs(), elapsed.as_millis(), len);
                log::info!("Finished multiple Blocks!");
                sched.tell_next(WORKER_DB, changes)?;
                // answer!(ctx, changes).map_err(|_| ArchiveError::from("Could not answer"))?;
            };
            ref broadcast: super::Broadcast => {
                match broadcast {
                    _ => break 'main,
                }
            };
            e: _ => log::warn!("Received unknown data {:?}", e);
        }
    }
    Ok(())
}
