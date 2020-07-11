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

use crate::database::{models::StorageModel, Database, DbConn};
use crate::error::ArchiveResult;
use crate::queries;
use crate::types::*;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use xtra::prelude::*;

impl Actor for Database {}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for Database
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blk: Block<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let mut conn = self.conn().await?;
        while !queries::check_if_meta_exists(blk.spec, &mut conn).await? {
            log::error!("METADATA DOESN'T EXIST, waiting");
            timer::Delay::new(std::time::Duration::from_millis(20)).await;
        }
        std::mem::drop(conn);
        self.insert(blk).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl<B> Handler<BatchBlock<B>> for Database
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(
        &mut self,
        mut blks: BatchBlock<B>,
        _ctx: &mut Context<Self>,
    ) -> ArchiveResult<()> {
        let specs = blks.mut_inner();
        specs.sort_by_key(|b| b.spec);
        let mut specs = specs.iter_mut().map(|b| b.spec).collect::<Vec<u32>>();
        specs.dedup();
        let mut conn = self.conn().await?;
        loop {
            let versions = queries::get_versions(&mut conn).await?;
            if db_contains_metadata(specs.as_slice(), versions) {
                break;
            }
            log::error!("Waiting....");
            timer::Delay::new(std::time::Duration::from_millis(50)).await;
        }
        std::mem::drop(conn);
        let now = std::time::Instant::now();
        self.insert(blks).await.map(|_| ())?;
        let elapsed = now.elapsed();
        log::debug!(
            "TOOK {} seconds, {} milli-seconds, {} micro-seconds, to insert blocks",
            elapsed.as_secs(),
            elapsed.as_millis(),
            elapsed.as_micros(),
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<Metadata> for Database {
    async fn handle(&mut self, meta: Metadata, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        self.insert(meta).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<Storage<B>> for Database {
    async fn handle(&mut self, storage: Storage<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let mut conn = self.conn().await?;
        while !queries::contains_block::<B>(*storage.hash(), &mut conn).await? {
            timer::Delay::new(std::time::Duration::from_millis(10)).await;
        }
        self.insert(Vec::<StorageModel<B>>::from(storage))
            .await
            .map(|_| ())
    }
}

pub struct VecStorageWrap<B: BlockT>(pub Vec<Storage<B>>);

impl<B: BlockT> Message for VecStorageWrap<B> {
    type Result = ArchiveResult<()>;
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<VecStorageWrap<B>> for Database {
    async fn handle(
        &mut self,
        storage: VecStorageWrap<B>,
        _ctx: &mut Context<Self>,
    ) -> ArchiveResult<()> {
        let mut conn = self.conn().await?;
        let block_nums: Vec<u32> = storage.0.iter().map(|s| s.block_num()).collect();
        while !queries::contains_blocks::<B>(block_nums.as_slice(), &mut conn).await? {
            timer::Delay::new(std::time::Duration::from_millis(50)).await;
        }
        let now = std::time::Instant::now();
        self.insert(Vec::<StorageModel<B>>::from(storage))
            .await
            .map(|_| ())?;
        let elapsed = now.elapsed();
        log::debug!(
            "TOOK {} seconds, {} milli-seconds, {} micro-seconds, to insert storage",
            elapsed.as_secs(),
            elapsed.as_millis(),
            elapsed.as_micros(),
        );
        Ok(())
    }
}

// Returns true if all versions are in database
// false if versions are missing
fn db_contains_metadata(specs: &[u32], versions: Vec<crate::queries::Version>) -> bool {
    let versions = versions
        .into_iter()
        .map(|v| v.version as u32)
        .collect::<Vec<u32>>();
    for spec in specs.iter() {
        if !versions.contains(spec) {
            return false;
        }
    }
    true
}

// this is an enum in case there is some more state
// that might be needed in the future
/// Get Some State from the Database Actor
pub enum GetState {
    Conn,
}

/// A resposne to `GetState`
/// it is callers responsiblity to make sure to call the
/// correct method on the implement after receiving the message
pub enum StateResponse {
    Conn(DbConn),
}

impl StateResponse {
    /// Pull a connection out of the enum
    ///
    /// # Panics
    /// panics if the enum is not actually of the `Conn` type
    pub fn conn(self) -> DbConn {
        match self {
            StateResponse::Conn(v) => v,
        }
    }
}

impl Message for GetState {
    type Result = ArchiveResult<StateResponse>;
}

#[async_trait::async_trait]
impl Handler<GetState> for Database {
    async fn handle(
        &mut self,
        msg: GetState,
        _: &mut Context<Self>,
    ) -> ArchiveResult<StateResponse> {
        match msg {
            GetState::Conn => {
                let conn = self.conn().await?;
                Ok(StateResponse::Conn(conn))
            }
        }
    }
}
