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
    BlockExecActor,
    actor_pool::ActorPool,
    exec_queue,
    workers::{DatabaseActor, GetState},
};

use crate::{error::Result, queries, sql_block_builder::BlockBuilder};
use sp_runtime::traits::Block as BlockT;
use sqlx::{pool::PoolConnection, Postgres};
use xtra::prelude::*;

#[derive(Clone)]
pub struct Generator<B: BlockT> {
    // could just use an atomic here
    addr: Address<ActorPool<DatabaseActor<B>>>,
    executor: Address<BlockExecActor<B>>
}

type Conn = PoolConnection<Postgres>;

impl<B: BlockT> Generator<B> {
    pub fn new(
        actor_pool: Address<ActorPool<DatabaseActor<B>>>,
        executor: Address<BlockExecActor<B>>,
        ) -> Self {
        Self {
            addr: actor_pool,
            executor
        }
    }

    ///  Spawn the tasks which collect un-indexed data
    pub fn start(self) -> Result<()> {
        crate::util::spawn(async move {
            let conn = self.addr.send(GetState::Conn.into()).await?.await?.conn();
            crate::util::spawn(Self::storage(conn, self.executor));
            Ok(())
        });
        Ok(())
    }

    /// Gets storage that is missing from the storage table
    /// by querying it against the blocks table
    /// This fills in storage that might've been missed by a shutdown
    async fn storage(mut conn: Conn, exec: Address<BlockExecActor<B>>) -> Result<()> {
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
        
        if let Err(_) = exec.send(exec_queue::BatchIn(blocks)).await {
            log::warn!("Block Executor channel disconnected before any missing storage-blocks could be sent")
        }
        Ok(())
    }
}
