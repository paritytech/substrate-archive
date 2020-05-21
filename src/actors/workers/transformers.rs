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

//! Actors which do work by decoding data before it's inserted into the database
//! these actors may do highly parallelized work
//! These actors do not make any external connections to a Database or Network
use crate::actors::scheduler::{Algorithm, Scheduler};
use crate::backend::ChainAccess;
use crate::types::*;
use bastion::prelude::*;
use sc_client_api::backend::StorageProvider;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as _, Header as _},
};
use sp_storage::{StorageData, StorageKey};
use sqlx::PgConnection;
use std::sync::Arc;
use subxt::system::System;

const REDUNDANCY: usize = 3;

// actor that takes blocks and transforms them into different types
pub fn actor<T, C>(client: Arc<C>, pool: sqlx::Pool<PgConnection>) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::BlockNumber: Into<u32>,
{
    let db = crate::database::Database::new(&pool).expect("Database intialization error");
    let db_workers = super::database::actor::<T>(db).expect("Couldn't start db workers");
    Bastion::children(|children: Children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let workers = db_workers.clone();
                let client = client.clone();
                async move {
                    log::info!("Transformer started");
                    let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: Block<T> =!> {
                                process_block(block.clone(), &mut sched).await;
                                // extract_storage(vec![block].as_slice(), &client, &mut sched).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("couldn't answer");
                             };
                             blocks: Vec<Block<T>> =!> {
                                 process_blocks(blocks.clone(), &mut sched).await;
                                 // extract_storage(blocks.as_slice(), &client, &mut sched).await;
                                 answer!(ctx, super::ArchiveAnswer::Success).expect("couldn't answer");
                             };
                            meta: Metadata =!> {
                                let v = sched.ask_next(meta).unwrap().await;
                                log::debug!("{:?}", v);
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        }
                    }
                }
            })
    })
}

pub async fn process_block<T>(block: Block<T>, sched: &mut Scheduler<'_>)
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    // TODO: join these futures
    let ext: Vec<Extrinsic<T>> = (&block).into();
    let v = sched.ask_next(block).unwrap().await;
    log::debug!("{:?}", v);
    let v = sched.ask_next(ext).unwrap().await;
    log::debug!("{:?}", v);
}

pub async fn process_blocks<T>(blocks: Vec<Block<T>>, sched: &mut Scheduler<'_>)
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    log::info!("Got {} blocks", blocks.len());
    //TODO: Join these futures
    let batch_blocks = BatchBlock::new(blocks.clone());
    let ext: Vec<Extrinsic<T>> = batch_blocks.into();
    log::info!("Processing blocks");
    let v = sched.ask_next(blocks).unwrap().await;
    log::debug!("{:?}", v);
    let v = sched.ask_next(ext).unwrap().await;
    log::debug!("{:?}", v);
}
