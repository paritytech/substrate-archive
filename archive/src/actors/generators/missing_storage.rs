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

//! Work generated from PostgreSQL database
//! Crawls for missing entires in the storage table
//! Gets storage changes based on those missing entries

use crate::actors::{
    scheduler::{Algorithm, Scheduler},
    workers,
};
use crate::backend::{BlockChanges, ThreadedBlockExecutor};
use crate::{
    error::Error as ArchiveError,
    queries,
    types::{NotSignedBlock, Storage, Substrate, System},
};
use bastion::prelude::*;
use crossbeam::channel;
use std::sync::Arc;

type BlockExecutor<T> = Arc<ThreadedBlockExecutor<NotSignedBlock<T>>>;

pub fn actor<T>(executor: BlockExecutor<T>, pool: sqlx::PgPool) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let workers = workers::transformers::<T>(pool.clone())?;
    // generate work from missing blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let executor = executor.clone();
            let workers = workers.clone();
            let pool = pool.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                sched.add_worker("transform", &workers);
                loop {
                    if handle_shutdown(&ctx).await {
                        break;
                    }
                    match entry::<T>(&executor, &pool, &mut sched).await {
                        Ok(_) => (),
                        Err(e) => log::error!("{:?}", e),
                    }
                }
                Bastion::stop();
                Ok(())
            }
        })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate database generator"))
}

async fn entry<T>(
    executor: &BlockExecutor<T>,
    pool: &sqlx::PgPool,
    sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let block_nums = queries::missing_storage(&pool).await?;
    log::info!("missing {} blocks", block_nums.len());
    if !(block_nums.len() > 0) {
        timer::Delay::new(std::time::Duration::from_secs(5)).await;
        return Ok(());
    }
    log::info!(
        "indexing {} missing storage entries, from {} to {} ...",
        block_nums.len(),
        block_nums[0].generate_series,
        block_nums[block_nums.len() - 1].generate_series
    );
    for num in block_nums.iter() {
        executor.push_to_queue(num.generate_series as u32);
    }
    let min = block_nums[0].generate_series;
    let max = block_nums[block_nums.len() - 1].generate_series;
    let count = block_nums.len() as u32;
    let now = std::time::Instant::now();
    while queries::get_present_in_range(&pool, min as u32, max as u32).await? < count {
        timer::Delay::new(std::time::Duration::from_secs(1)).await;

        let block_changes = executor
            .receiver()
            .try_iter()
            .map(|c| Storage::<T>::from(c))
            .collect::<Vec<Storage<T>>>();

        log::info!("Syncing Storage {} bps", block_changes.len());

        if block_changes.len() > 0 {
            let answer = sched.tell_next("transform", block_changes)?;
            log::debug!("{:?}", answer);
        }
    }
    let elapsed = now.elapsed();
    log::info!(
        "took {} seconds to execute {} blocks",
        elapsed.as_secs(),
        count
    );

    Ok(())
}

// Handle a shutdown
async fn handle_shutdown(ctx: &BastionContext) -> bool {
    if let Some(msg) = ctx.try_recv().await {
        msg! {
            msg,
            broadcast: super::Broadcast => {
                match broadcast {
                    super::Broadcast::Shutdown => {
                        return true;
                    }
                }
            };
            e: _ => log::warn!("Received unknown message: {:?}", e);
        };
    }
    false
}
