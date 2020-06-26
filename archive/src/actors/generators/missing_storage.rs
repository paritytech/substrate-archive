// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

use crate::{
    actors::ActorContext,
    backend::{BlockBroker, BlockData},
    sql_block_builder::BlockBuilder,
};
use crate::{
    error::Error as ArchiveError,
    queries,
    types::{NotSignedBlock, Storage, Substrate, System},
};

pub async fn storage_loop<T>(context: ActorContext<T>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    on_start(&context).await?;
    main_loop(context).await?;
    Ok(())
}

async fn on_start<T>(context: &ActorContext<T>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    if queries::blocks_count(&context.pool()).await? <= 0 {
        // no blocks means we haven't indexed anything yet
        return Ok(());
    }
    let now = std::time::Instant::now();
    let blocks = queries::blocks_storage_intersection(&context.pool()).await?;
    let blocks = BlockBuilder::new().with_vec(blocks)?;
    let elapsed = now.elapsed();
    log::info!(
        "TOOK {} seconds, {} milli-seconds to get and build {} blocks",
        elapsed.as_secs(),
        elapsed.as_millis(),
        blocks.len()
    );
    log::info!("indexing {} blocks of storage ... ", blocks.len());
    context.broker().work.send(BlockData::Batch(blocks)).unwrap();
    Ok(())
}

async fn main_loop<T>(context: ActorContext<T>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    loop {
        timer::Delay::new(std::time::Duration::from_secs(1)).await;
        let count = check_work::<T>(context.broker())?;
        if count > 0 {
            log::info!("Syncing Storage {} bps", count);
        }
    }
    Ok(())
}

/// Check the receiver end of the BlockExecution ThreadPool for any storage
/// changes resulting from block execution.
fn check_work<T>(broker: &BlockBroker<NotSignedBlock<T>>) -> Result<usize, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let block_changes = broker
        .results
        .try_iter()
        .map(|c| Storage::<T>::from(c))
        .collect::<Vec<Storage<T>>>();

    if block_changes.len() > 0 {
        let count = block_changes.len();
        // sched.tell_next("transform", block_changes)?;
        Ok(count)
    } else {
        Ok(0)
    }
}
