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
    workers::{Aggregator, DatabaseActor, GetState},
};
use crate::{backend::ReadOnlyBackend, error::ArchiveResult, queries};
use sp_runtime::generic::SignedBlock;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use sqlx::{pool::PoolConnection, Postgres};
use std::{marker::PhantomData, sync::Arc};
use xtra::prelude::*;

type DatabaseAct<B> = Address<ActorPool<DatabaseActor<B>>>;
type Conn = PoolConnection<Postgres>;

pub struct BlocksIndexer<B: BlockT>
where
    NumberFor<B>: Into<u32>,
{
    /// background task to crawl blocks
    backend: Arc<ReadOnlyBackend<B>>,
    db: DatabaseAct<B>,
    ag: Address<Aggregator<B>>,
}

impl<B: BlockT> BlocksIndexer<B>
where
    NumberFor<B>: Into<u32>,
{
    pub async fn new(
        backend: Arc<ReadOnlyBackend<B>>,
        addr: DatabaseAct<B>,
        ag: Address<Aggregator<B>>,
    ) -> ArchiveResult<Self> {
        Ok(Self {
            backend,
            db: addr,
            ag,
        })
    }

    pub async fn crawl_blocks(&self) -> ArchiveResult<()> {
        let last_max = 0;
        let mut conn = self.db.send(GetState::Conn.into()).await?.await?.conn();

        loop {
            let numbers = queries::missing_blocks_min_max(&mut conn, last_max).await?;
            let backend = self.backend.clone();
            let blocks = smol::unblock!(move || -> ArchiveResult<Vec<SignedBlock<B>>> {
                Ok(backend
                    .iter_blocks(|n| numbers.contains(&n))?
                    .collect::<Vec<SignedBlock<B>>>())
            });
        }
        Ok(())
    }
}
