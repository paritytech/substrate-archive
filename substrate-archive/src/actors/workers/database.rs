// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use futures_timer::Delay;
use std::time::Duration;

use sp_runtime::traits::{Block as BlockT, NumberFor};

use xtra::prelude::*;

use crate::{
	database::{models::StorageModel, queries, Database, DbConn},
	error::Result,
	types::{BatchBlock, BatchStorage, Block, Metadata, Storage},
	wasm_tracing::Traces,
};

#[derive(Clone)]
pub struct DatabaseActor {
	db: Database,
}

impl DatabaseActor {
	pub fn new(url: String) -> Result<Self> {
		Ok(Self { db: Database::new(url)? })
	}

	async fn block_handler<B>(&self, blk: Block<B>) -> Result<()>
	where
		B: BlockT,
		NumberFor<B>: Into<u32>,
	{
		let mut conn = self.db.conn().await?;
		while !queries::check_if_meta_exists(blk.spec, &mut conn).await? {
			Delay::new(Duration::from_millis(20)).await;
		}
		std::mem::drop(conn);
		self.db.insert(blk).await?;
		Ok(())
	}

	// Returns true if all versions are in database
	// false if versions are missing
	async fn db_contains_metadata<B>(blocks: &[Block<B>], conn: &mut DbConn) -> Result<bool> {
		let specs: hashbrown::HashSet<u32> = blocks.iter().map(|b| b.spec).collect();
		let versions: hashbrown::HashSet<u32> = queries::get_versions(conn).await?.into_iter().collect();
		Ok(specs.is_subset(&versions))
	}

	async fn batch_block_handler<B>(&self, blks: BatchBlock<B>) -> Result<()>
	where
		B: BlockT,
		NumberFor<B>: Into<u32>,
	{
		let mut conn = self.db.conn().await?;
		while !Self::db_contains_metadata(blks.inner(), &mut conn).await? {
			log::info!("Doesn't contain metadata");
			Delay::new(Duration::from_millis(50)).await;
		}
		std::mem::drop(conn);
		self.db.insert(blks).await?;
		Ok(())
	}

	async fn storage_handler<H>(&self, storage: Storage<H>) -> Result<()>
	where
		H: Send + Sync + Copy + AsRef<[u8]> + 'static,
	{
		let mut conn = self.db.conn().await?;
		while !queries::has_block::<H>(*storage.hash(), &mut conn).await? {
			Delay::new(Duration::from_millis(10)).await;
		}
		let storage = Vec::<StorageModel<H>>::from(storage);
		std::mem::drop(conn);
		self.db.insert(storage).await?;
		Ok(())
	}

	async fn batch_storage_handler<H>(&self, storages: BatchStorage<H>) -> Result<()>
	where
		H: Send + Sync + Copy + AsRef<[u8]> + 'static,
	{
		let mut conn = self.db.conn().await?;
		let mut block_nums = storages.inner().iter().map(|s| s.block_num()).collect::<Vec<_>>();
		block_nums.sort_unstable();
		if !block_nums.is_empty() {
			log::info!("Inserting: {:#?}, {} .. {}", block_nums.len(), block_nums[0], block_nums.last().unwrap());
		}
		let len = block_nums.len();
		let now = std::time::Instant::now();
		while queries::has_blocks(block_nums.as_slice(), &mut conn).await?.len() != len {
			Delay::new(std::time::Duration::from_millis(50)).await;
		}
		log::debug!("Insert Integrity Query Check took {:?}", now.elapsed());
		// we drop the connection early so that the insert() has the use of all db connections
		std::mem::drop(conn);
		let storage = Vec::<StorageModel<H>>::from(storages);
		let now = std::time::Instant::now();
		self.db.concurrent_insert(storage).await?;
		log::debug!("[Batch Storage Insert] took {:?}", now.elapsed());
		Ok(())
	}
}

impl Actor for DatabaseActor {}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for DatabaseActor
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
impl<B> Handler<BatchBlock<B>> for DatabaseActor
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
			log::info!("Took {:?} to insert {} blocks", now.elapsed(), len);
		} else {
			log::debug!("Took {:?} to insert {} blocks", now.elapsed(), len);
		}
	}
}

#[async_trait::async_trait]
impl Handler<Metadata> for DatabaseActor {
	async fn handle(&mut self, meta: Metadata, _ctx: &mut Context<Self>) {
		if let Err(e) = self.db.insert(meta).await {
			log::error!("{}", e.to_string());
		}
	}
}

#[async_trait::async_trait]
impl<H> Handler<Storage<H>> for DatabaseActor
where
	H: Copy + Send + Sync + AsRef<[u8]> + 'static,
{
	async fn handle(&mut self, storage: Storage<H>, _ctx: &mut Context<Self>) {
		if let Err(e) = self.storage_handler(storage).await {
			log::error!("{}", e.to_string())
		}
	}
}

#[async_trait::async_trait]
impl<H> Handler<BatchStorage<H>> for DatabaseActor
where
	H: Copy + Send + Sync + AsRef<[u8]> + 'static,
{
	async fn handle(&mut self, storages: BatchStorage<H>, _ctx: &mut Context<Self>) {
		let len = storages.inner.iter().map(|storage| storage.changes.len()).sum::<usize>();
		let now = std::time::Instant::now();
		if let Err(e) = self.batch_storage_handler(storages).await {
			log::error!("{}", e.to_string());
		}

		if now.elapsed() > std::time::Duration::from_millis(5000) {
			log::warn!("Took {:?} to insert {} storage entries", now.elapsed(), len);
		}
	}
}

impl Message for Traces {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<Traces> for DatabaseActor {
	async fn handle(&mut self, traces: Traces, _: &mut Context<Self>) {
		let now = std::time::Instant::now();
		if let Err(e) = self.db.insert(traces).await {
			log::error!("{}", e.to_string());
		}
		log::debug!("took {:?} to insert traces", now.elapsed());
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
impl Handler<GetState> for DatabaseActor {
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
