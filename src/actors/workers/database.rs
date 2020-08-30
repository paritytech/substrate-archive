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

use crate::actors::msg::VecStorageWrap;
use crate::database::{Database, DbConn, StorageModel};
use crate::error::Result;
use crate::queries;
use crate::types::{BatchBlock, Block, Metadata, Storage};
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::marker::PhantomData;
use std::time::Duration;
use xtra::prelude::*;

#[derive(Clone)]
pub struct DatabaseActor<B: BlockT> {
    db: Database,
    _marker: PhantomData<B>,
}

impl<B: BlockT> DatabaseActor<B> {
    pub async fn new(url: String) -> Result<Self> {
        Ok(Self {
            db: Database::new(url).await?,
            _marker: PhantomData,
        })
    }

    #[allow(unused)]
    pub fn with_db(db: Database) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    async fn block_handler(&self, blk: Block<B>) -> Result<()>
    where
        NumberFor<B>: Into<u32>,
    {
        let mut conn = self.db.conn().await?;
        while !queries::check_if_meta_exists(blk.spec, &mut conn).await? {
            smol::Timer::new(Duration::from_millis(20)).await;
        }
        std::mem::drop(conn);
        self.db.insert(blk).await?;
        Ok(())
    }

    // Returns true if all versions are in database
    // false if versions are missing
    async fn db_contains_metadata(blocks: &[Block<B>], conn: &mut DbConn) -> Result<bool> {
        let specs: hashbrown::HashSet<u32> = blocks.iter().map(|b| b.spec).collect();
        let versions: hashbrown::HashSet<u32> =
            queries::get_versions(conn).await?.into_iter().collect();
        Ok(specs.is_subset(&versions))
    }

    async fn batch_block_handler(&self, blks: BatchBlock<B>) -> Result<()>
    where
        NumberFor<B>: Into<u32>,
    {
        let mut conn = self.db.conn().await?;
        while !Self::db_contains_metadata(blks.inner(), &mut conn).await? {
            log::info!("Doesn't contain metadata");
            smol::Timer::new(Duration::from_millis(50)).await;
        }
        std::mem::drop(conn);
        self.db.insert(blks).await?;
        Ok(())
    }

    async fn storage_handler(&self, storage: Storage<B>) -> Result<()> {
        let mut conn = self.db.conn().await?;
        while !queries::has_block::<B>(*storage.hash(), &mut conn).await? {
            smol::Timer::new(Duration::from_millis(10)).await;
        }
        let storage = Vec::<StorageModel<B>>::from(storage);
        std::mem::drop(conn);
        self.db.insert(storage).await?;
        Ok(())
    }

    async fn batch_storage_handler(&self, storage: Vec<Storage<B>>) -> Result<()> {
        let mut conn = self.db.conn().await?;
        let mut block_nums: Vec<u32> = storage.iter().map(|s| s.block_num()).collect();
        block_nums.sort();
        log::debug!(
            "Inserting: {:#?}, {} .. {}",
            block_nums.len(),
            block_nums[0],
            block_nums.last().unwrap()
        );
        let len = block_nums.len();
        while queries::has_blocks::<B>(block_nums.as_slice(), &mut conn)
            .await?
            .len()
            != len
        {
            smol::Timer::new(std::time::Duration::from_millis(50)).await;
        }
        // we drop the connection early so that the insert() has the use of all db connections
        std::mem::drop(conn);
        let storage = Vec::<StorageModel<B>>::from(VecStorageWrap(storage));
        self.db.insert(storage).await?;
        Ok(())
    }
}

impl<B: BlockT> Actor for DatabaseActor<B> {}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for DatabaseActor<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blk: Block<B>, _: &mut Context<Self>) {
        if let Err(e) = self.block_handler(blk).await {
            log::error!("{}", e.to_string())
        }
    }
}

#[async_trait::async_trait]
impl<B> Handler<BatchBlock<B>> for DatabaseActor<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blks: BatchBlock<B>, _: &mut Context<Self>) {
        let len = blks.inner.len();
        let now = std::time::Instant::now();
        if let Err(e) = self.batch_block_handler(blks).await {
            log::error!("{}", e.to_string());
        }
        if len > 1000 {
            log::info!("took {:?} to insert {} blocks", now.elapsed(), len);
        } else {
            log::debug!("took {:?} to insert {} blocks", now.elapsed(), len);
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<Metadata> for DatabaseActor<B> {
    async fn handle(&mut self, meta: Metadata, _ctx: &mut Context<Self>) {
        if let Err(e) = self.db.insert(meta).await {
            log::error!("{}", e.to_string());
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<Storage<B>> for DatabaseActor<B> {
    async fn handle(&mut self, storage: Storage<B>, _ctx: &mut Context<Self>) {
        if let Err(e) = self.storage_handler(storage).await {
            log::error!("{}", e.to_string())
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<VecStorageWrap<B>> for DatabaseActor<B> {
    async fn handle(&mut self, storage: VecStorageWrap<B>, _ctx: &mut Context<Self>) {
        let now = std::time::Instant::now();
        if let Err(e) = self.batch_storage_handler(storage.0).await {
            log::error!("{}", e.to_string());
        }
        log::debug!("took {:?} to insert storage", now.elapsed());
    }
}

// this is an enum in case there is some more state
// that might be needed in the future
/// Get Some State from the Database Actor
#[derive(Debug)]
pub enum GetState {
    // Get a single connection
    Conn,
    // Get the Connection Pool
    Pool,
}

/// A response to `GetState`
/// it is callers responsiblity to make sure to call the
/// correct method on the implement after receiving the message
#[derive(Debug)]
pub enum StateResponse {
    Conn(DbConn),
    Pool(sqlx::PgPool),
}

impl StateResponse {
    /// Pull a connection out of the enum
    ///
    /// # Panics
    /// panics if the enum is not actually of the `Conn` type
    pub fn conn(self) -> DbConn {
        match self {
            StateResponse::Conn(v) => v,
            StateResponse::Pool(_) => panic!("Not a connection"),
        }
    }

    /// Pull a pool out of the enum
    ///
    /// # Panics
    /// panics if the enum is not actually of the 'pool' type
    pub fn pool(self) -> sqlx::PgPool {
        match self {
            StateResponse::Pool(v) => v,
            StateResponse::Conn(_) => panic!("Not a pool"),
        }
    }
}

impl Message for GetState {
    type Result = Result<StateResponse>;
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<GetState> for DatabaseActor<B> {
    async fn handle(&mut self, msg: GetState, _: &mut Context<Self>) -> Result<StateResponse> {
        match msg {
            GetState::Conn => {
                let conn = self.db.conn().await?;
                Ok(StateResponse::Conn(conn))
            }
            GetState::Pool => {
                let pool = self.db.pool().clone();
                Ok(StateResponse::Pool(pool))
            }
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<super::Die> for DatabaseActor<B>
where
    NumberFor<B>: Into<u32>,
    B::Hash: Unpin,
{
    async fn handle(&mut self, _: super::Die, ctx: &mut Context<Self>) -> Result<()> {
        ctx.stop();
        Ok(())
    }
}
