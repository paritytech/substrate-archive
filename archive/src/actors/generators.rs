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
use crate::{error::Result, queries, sql_block_builder::BlockBuilder, threadpools::BlockData};
use flume::Sender;
use sp_runtime::traits::Block as BlockT;
use sqlx::{pool::PoolConnection, Postgres};
use xtra::prelude::*;

#[derive(Clone)]
pub struct Generator<B: BlockT> {
    last_block_max: u32,
    addr: Address<ActorPool<DatabaseActor<B>>>,
    tx_block: Sender<BlockData<B>>,
}

type Conn = PoolConnection<Postgres>;

impl<B: BlockT> Generator<B> {
    pub fn new(
        actor_pool: Address<ActorPool<DatabaseActor<B>>>,
        tx_block: Sender<BlockData<B>>,
    ) -> Self {
        Self {
            last_block_max: 0,
            addr: actor_pool,
            tx_block,
        }
    }

    ///  Spawn the tasks which collect un-indexed data
    pub fn start(self) -> Result<()> {
        let tx_block = self.tx_block.clone();
        crate::util::spawn(async move {
            let conn0 = self.addr.send(GetState::Conn.into()).await?.await?.conn();
            let conn1 = self.addr.send(GetState::Conn.into()).await?.await?.conn();
            crate::util::spawn(Self::storage(conn0, self.tx_block));
            Ok(())
        });
        Ok(())
    }

    /// Gets storage that is missing from the storage table
    /// by querying it against the blocks table
    /// This fills in storage that might've been missed by a shutdown
    async fn storage(mut conn: Conn, tx_block: Sender<BlockData<B>>) -> Result<()> {
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
