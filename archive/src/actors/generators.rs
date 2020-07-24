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
use std::sync::Arc;
use xtra::prelude::*;

#[derive(Clone)]
pub struct Generator<B: BlockT> {
    // could just use an atomic here
    last_block_max: Arc<u32>,
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
            last_block_max: Arc::new(0),
            addr: actor_pool,
            tx_block,
            tx_num,
        }
    }

    pub async fn start(self) -> ArchiveResult<()> {
        let conn0 = self.addr.send(GetState::Conn.into()).await?.await?.conn();
        let conn1 = self.addr.send(GetState::Conn.into()).await?.await?.conn();
        crate::util::spawn(self.clone().storage(conn0));
        crate::util::spawn(self.missing_blocks(conn1));
        Ok(())
    }

    /// Every second gets blocks missing from database
    async fn missing_blocks(mut self, mut conn: Conn) -> ArchiveResult<()> {
        'gen: loop {
            let numbers = queries::missing_blocks_min_max(&mut conn, *self.last_block_max).await?;
            let max = if !numbers.is_empty() {
                log::info!(
                    "Indexing {} missing blocks, from {} to {}...",
                    numbers.len(),
                    numbers.first().unwrap(),
                    numbers.last().unwrap()
                );
                numbers[numbers.len() - 1]
            } else {
                *self.last_block_max
            };
            for num in numbers.iter() {
                if let Err(_) = self.tx_num.send(*num) {
                    // threadpool has disconnected so we can stop
                    break 'gen;
                }
            }
            if let Some(old_max) = std::sync::Arc::<u32>::get_mut(&mut self.last_block_max) {
                log::debug!("new max: {}", max);
                *old_max = max;
            }
            if numbers.is_empty() {
                timer::Delay::new(std::time::Duration::from_secs(5)).await;
            } else {
                timer::Delay::new(std::time::Duration::from_secs(1)).await;
            }
        }
        Ok(())
    }

    /// Gets storage that is missing from the storage table
    /// by querying it against the blocks table
    /// This fills in storage that might've been missed by a shutdown
    async fn storage(self, mut conn: Conn) -> ArchiveResult<()> {
        if queries::blocks_count(&mut conn).await? == 0 {
            // no blocks means we haven't indexed anything yet
            return Ok(());
        }
        let now = std::time::Instant::now();
        let blocks = queries::blocks_storage_intersection(&mut conn).await?;
        let blocks = BlockBuilder::<B>::new().with_vec(blocks)?;
        log::info!("took {:?} to get and build blocks", now.elapsed());

        log::info!("indexing {} blocks of storage ... ", blocks.len());
        self.tx_block.send(BlockData::Batch(blocks))?;
        Ok(())
    }
}
