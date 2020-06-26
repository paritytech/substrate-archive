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
    actors::{
        workers::{
            self,
            msg::{StorageWrap, VecStorageWrap},
        },
        ActorContext,
    },
    backend::{BlockBroker, BlockData},
    error::ArchiveResult,
    queries,
    sql_block_builder::BlockBuilder,
    types::{NotSignedBlock, Storage, Substrate, System},
};
use xtra::prelude::*;

pub struct MissingStorage<T: Substrate> {
    context: ActorContext<T>,
    addr: Address<workers::Transform>,
}

impl<T: Substrate> MissingStorage<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    /// create a new MissingStorage Indexer
    /// Must be run within the context of an executor
    pub fn new(context: ActorContext<T>) -> Self {
        let addr = workers::Transform::default().spawn();
        Self { context, addr }
    }

    pub async fn storage_loop(self) -> ArchiveResult<()> {
        let addr = workers::Transform::default().spawn();
        self.on_start().await?;
        self.main_loop().await?;
        Ok(())
    }

    async fn on_start(&self) -> ArchiveResult<()> {
        if queries::blocks_count(&self.context.pool()).await? <= 0 {
            // no blocks means we haven't indexed anything yet
            return Ok(());
        }
        let now = std::time::Instant::now();
        let blocks = queries::blocks_storage_intersection(&self.context.pool()).await?;
        let blocks = BlockBuilder::new().with_vec(blocks)?;
        let elapsed = now.elapsed();
        log::info!(
            "TOOK {} seconds, {} milli-seconds to get and build {} blocks",
            elapsed.as_secs(),
            elapsed.as_millis(),
            blocks.len()
        );
        log::info!("indexing {} blocks of storage ... ", blocks.len());
        self.context.broker().work.send(BlockData::Batch(blocks)).unwrap();
        Ok(())
    }

    async fn main_loop(&self) -> ArchiveResult<()> {
        loop {
            timer::Delay::new(std::time::Duration::from_secs(1)).await;
            let count = self.check_work()?;
            if count > 0 {
                log::info!("Syncing Storage {} bps", count);
            }
        }
        Ok(())
    }

    /// Check the receiver end of the BlockExecution ThreadPool for any storage
    /// changes resulting from block execution.
    fn check_work(&self) -> ArchiveResult<usize> {
        let block_changes = self
            .context
            .broker
            .results
            .try_iter()
            .map(|c| Storage::<T>::from(c))
            .collect::<Vec<Storage<T>>>();

        if block_changes.len() > 0 {
            let count = block_changes.len();
            self.addr.send(VecStorageWrap::from(block_changes));
            Ok(count)
        } else {
            Ok(0)
        }
    }
}
