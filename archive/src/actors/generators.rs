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

//! Holds futures that generate data to be processed by threadpools/actors

use super::{
    actor_pool::ActorPool,
    workers::{DatabaseActor, GetState},
};
use crate::{
    error::ArchiveResult, queries, sql_block_builder::BlockBuilder, threadpools::BlockData,
};
use flume::Sender;
use sp_runtime::traits::Block as BlockT;
use sqlx::{pool::PoolConnection, Postgres};
use xtra::prelude::*;

#[derive(Clone)]
pub struct Generator<B: BlockT> {
    last_block_max: u32,
    addr: Address<ActorPool<DatabaseActor<B>>>,
    tx_block: Sender<BlockData<B>>,
    tx_num: Sender<u32>,
}

type Conn = PoolConnection<Postgres>;

impl<B: BlockT> Generator<B> {
    pub fn new(
        actor_pool: Address<ActorPool<DatabaseActor<B>>>,
        tx_block: Sender<BlockData<B>>,
        tx_num: Sender<u32>,
    ) -> Self {
        Self {
            last_block_max: 0,
            addr: actor_pool,
            tx_block,
            tx_num,
        }
    }

    ///  Spawn the tasks which collect un-indexed data
    pub fn start(self) -> ArchiveResult<()> {
        let tx_num = self.tx_num.clone();
        let tx_block = self.tx_block.clone();
        crate::util::spawn(async move {
            let conn0 = self.addr.send(GetState::Conn.into()).await?.await?.conn();
            let conn1 = self.addr.send(GetState::Conn.into()).await?.await?.conn();
            crate::util::spawn(Self::storage(conn0, self.tx_block));
            // Self::missing_blocks(conn1, self.tx_num).await?;
            Ok(())
        });
        Ok(())
    }
    /*
    /// gets blocks from the database on a 1-seconds interval
    /// if the last query returned no missing blocks, the next interval will be 5 seconds
    async fn missing_blocks(mut conn: Conn, tx_num: Sender<u32>) -> ArchiveResult<()> {
        let mut last_block_max = 0;
        'gen: loop {
            let numbers = queries::missing_blocks_min_max(&mut conn, last_block_max).await?;
            let max = if !numbers.is_empty() {
                numbers[numbers.len() - 1]
            } else {
                last_block_max
            };

            if max != last_block_max {
                log::info!(
                    "Indexing {} missing blocks, from {} to {}...",
                    numbers.len(),
                    numbers.first().unwrap(),
                    numbers.last().unwrap()
                );
                for num in numbers.iter() {
                    if let Err(_) = tx_num.send(*num) {
                        // threadpool has disconnected so we can stop
                        break 'gen;
                    }
                }
                log::debug!("new max: {}", max);
                last_block_max = max;
            }

            if tx_num.try_send(0).is_err() {
                break 'gen;
            }

            if numbers.is_empty() {
                timer::Delay::new(std::time::Duration::from_secs(5)).await;
            } else {
                timer::Delay::new(std::time::Duration::from_secs(1)).await;
            }
        }
        Ok(())
    }
    */

    /// Gets storage that is missing from the storage table
    /// by querying it against the blocks table
    /// This fills in storage that might've been missed by a shutdown
    async fn storage(mut conn: Conn, tx_block: Sender<BlockData<B>>) -> ArchiveResult<()> {
        if queries::blocks_count(&mut conn).await? == 0 {
            // no blocks means we haven't indexed anything yet
            return Ok(());
        }
        let now = std::time::Instant::now();
        let blocks = queries::blocks_storage_intersection(&mut conn).await?;
        let blocks = BlockBuilder::<B>::new().with_vec(blocks)?;
        log::info!(
            "took {:?} to get and build {} blocks. Adding to queue...",
            now.elapsed(),
            blocks.len()
        );

        if let Err(_) = tx_block.send(BlockData::Batch(blocks)) {
            log::warn!("Block Executor channel disconnected before any missing storage-blocks could be sent")
        }
        Ok(())
    }
}
