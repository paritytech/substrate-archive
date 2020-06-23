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

//! Actors which do work by transforming data before it's inserted into the database
//! These actors do not make any external connections to a Database or Network
use crate::{
    actors::scheduler::{Algorithm, Scheduler},
    error::Error as ArchiveError,
    print_on_err,
    types::*,
};
use bastion::prelude::*;
use sp_core::storage::StorageChangeSet;
use sqlx::PgConnection;

const REDUNDANCY: usize = 2;

// actor that takes blocks and transforms them into different types
pub fn actor<T>(pool: sqlx::Pool<PgConnection>) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let db = crate::database::Database::new(&pool)?;
    let db_workers = super::database::actor::<T>(db)?;
    Bastion::children(|children: Children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = db_workers.clone();
                async move {
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                    sched.add_worker("db", &workers);
                    print_on_err!(handle_msg::<T>(&mut sched).await);
                    Ok(())
                }
            })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate database actor"))
}

pub async fn handle_msg<T>(sched: &mut Scheduler<'_>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    loop {
        msg! {
            sched.context().recv().await.expect("Could not receive"),
            block: Block<T> =!> {
                process_block(block, sched).await?;
                crate::archive_answer!(sched.context(), super::ArchiveAnswer::Success)?;
            };
            blocks: Vec<Block<T>> =!> {
                process_blocks(blocks, sched).await?;
                crate::archive_answer!(sched.context(), super::ArchiveAnswer::Success)?;
            };
            meta: Metadata =!> {
                // we ask here because metadata needs to be inserted
                // before blocks. This gives the caller the opportunity to wait for
                // that event as well
                let v = sched.ask_next("db", meta)?.await;
                log::debug!("{:?}", v);
            };
            storage: (<T as System>::BlockNumber, StorageChangeSet<<T as System>::Hash>) => {
                let (num, changes) = storage;
                process_storage::<T>(num, changes, sched).await?;
            };
            bulk_storage: Vec<Storage<T>> => {
                for s in bulk_storage.into_iter() {
                    let v = sched.ask_next("db", s)?.await;
                    log::debug!("{:?}", v);
                }
            };
            ref _broadcast: super::Broadcast => {
                ()
            };
            e: _ => log::warn!("Received unknown data {:?}", e);
        }
    }
}

pub async fn process_storage<T>(
    num: <T as System>::BlockNumber,
    changes: StorageChangeSet<<T as System>::Hash>,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let hash = changes.block;
    let num: u32 = num.into();
    let storage = Storage::<T>::new(hash, num, false, changes.changes);
    let v = sched.ask_next("db", storage)?.await;
    log::debug!("{:?}", v);
    Ok(())
}

pub async fn process_block<T>(
    block: Block<T>,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    // blocks need to be inserted before extrinsics, so that extrinsics may reference block hash in postgres
    let v = sched.ask_next("db", block)?.await;
    log::debug!("{:?}", v);
    Ok(())
}

pub async fn process_blocks<T>(
    blocks: Vec<Block<T>>,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    log::info!("Got {} blocks", blocks.len());

    let batch_blocks = BatchBlock::new(blocks);
    // blocks need to be inserted before extrinsics, so that extrinsics may reference block hash in postgres
    log::info!("Processing blocks");
    let v = sched.ask_next("db", batch_blocks)?.await;
    log::debug!("{:?}", v);
    Ok(())
}
