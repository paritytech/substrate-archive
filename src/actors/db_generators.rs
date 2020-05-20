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

//! Work Generated and gathered from the PostgreSQL Database
//! IE: Missing Blocks/Storage/Inherents/Transactions
//! Gathers Missing blocks -> passes to metadata -> passes to extractors -> passes to decode -> passes to insert

use super::scheduler::{Algorithm, Scheduler};
use crate::{
    backend::ChainAccess,
    database::Insert,
    error::Error as ArchiveError,
    queries,
    types::{BatchBlock, NotSignedBlock, Substrate},
};
use async_std::prelude::*;
use async_std::stream;
use bastion::prelude::*;
use bigdecimal::ToPrimitive;
use sc_client_api::client::BlockBackend as _;
use sp_runtime::generic::BlockId;
use sqlx::PgConnection;
use subxt::system::System;
use std::{sync::Arc, time::Duration};

const DURATION: u64 = 5;

pub fn actor<T, C>(
    client: Arc<C>,
    pool: sqlx::Pool<PgConnection>,
    url: String,
) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::BlockNumber: Into<u32>,
{
    let meta_workers = super::metadata::actor::<T, _>(url.clone(), pool.clone(), client.clone())
        .expect("Couldn't start metadata workers");
    // generate work from missing blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let client = client.clone();
            let pool = pool.clone();
            let workers = meta_workers.clone();
            let url = url.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx, &workers);
                loop {
                    match entry::<T, _>(&client, &pool, url.as_str(), &mut sched).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e)
                    }
                }
                Ok(())
            }
        })
    })
}

async fn entry<T, C>(client: &Arc<C>,
                     pool: &sqlx::Pool<PgConnection>,
                     url: &str,
                     sched: &mut Scheduler<'_>
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    C: ChainAccess<NotSignedBlock> + 'static,
    <T as System>::BlockNumber: Into<u32>

{
    let mut block_nums = queries::missing_blocks(&pool).await?;
    log::info!("BLOCK NUM LENGTH: {}", block_nums.len());
    if !(block_nums.len() > 0) {
        async_std::task::sleep(Duration::from_secs(DURATION));
        return Ok(());
    }
    log::info!(
        "Starting to crawl for {} missing blocks, from {} .. {} ...",
        block_nums.len(),
        block_nums[0].generate_series,
        block_nums[block_nums.len() - 1].generate_series
    );
    let mut blocks = Vec::new();
    for block_num in block_nums.iter() {
        let num = block_num
            .generate_series
            .to_u32()
            .expect("Could not convert into u32");
        let b = client
            .block(&BlockId::Number(num))
            .expect("Error getting block");
        if b.is_none() {
            log::warn!("Block does not exist!")
        } else {
            blocks.push(b.expect("Checked for none; qed"));
        }
    }
    log::info!("Got {} blocks", blocks.len());
    let answer = sched.ask_next(blocks).unwrap().await;
    log::debug!("{:?}", answer);
    Ok(())
}
