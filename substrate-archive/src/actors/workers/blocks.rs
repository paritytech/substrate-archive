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

use std::sync::Arc;

use xtra::prelude::*;

use sp_runtime::{
	generic::{BlockId, SignedBlock},
	traits::{Block as BlockT, Header as _, NumberFor},
};
use substrate_archive_backend::{ReadOnlyBackend, ReadOnlyDB, RuntimeVersionCache};

use crate::{
	actors::{
		actor_pool::ActorPool,
		workers::{
			database::{DatabaseActor, GetState},
			metadata::MetadataActor,
		},
		SystemConfig,
	},
	database::queries,
	error::{ArchiveError, Result},
	types::{BatchBlock, Block, Die},
};

type DatabaseAct<B> = Address<ActorPool<DatabaseActor<B>>>;
type MetadataAct<B> = Address<MetadataActor<B>>;

pub struct BlocksIndexer<B: BlockT, D>
where
	D: ReadOnlyDB,
	NumberFor<B>: Into<u32>,
	B: Unpin,
	B::Hash: Unpin,
{
	/// background task to crawl blocks
	backend: Arc<ReadOnlyBackend<B, D>>,
	db: DatabaseAct<B>,
	meta: MetadataAct<B>,
	rt_cache: Arc<RuntimeVersionCache<B, D>>,
	/// the last maximum block number from which we are sure every block before then is indexed
	last_max: u32,
	/// the maximum amount of blocks to index at once
	max_block_load: u32,
}

impl<B: BlockT + Unpin, D: ReadOnlyDB + 'static> BlocksIndexer<B, D>
where
	B::Hash: Unpin,
	NumberFor<B>: Into<u32>,
{
	pub fn new(conf: &SystemConfig<B, D>, db: DatabaseAct<B>, meta: MetadataAct<B>) -> Self {
		Self {
			rt_cache: Arc::new(RuntimeVersionCache::new(conf.backend.clone())),
			last_max: 0,
			backend: conf.backend().clone(),
			db,
			meta,
			max_block_load: conf.control.max_block_load,
		}
	}

	async fn fetch_genesis_block(&self) -> Result<Option<Block<B>>>
	where
		NumberFor<B>: From<u32>,
	{
		let backend = self.backend.clone();
		if let Some(block) = backend.block(&BlockId::Number(0.into())) {
			let cache = self.rt_cache.clone();
			let spec = cache.get(block.block.hash())?.map(|ver| ver.spec_version).unwrap_or_default();
			Ok(Some(Block { inner: block, spec }))
		} else {
			Ok(None)
		}
	}

	async fn fetch_and_send_genesis_block(&self) -> Result<()> {
		if let Some(genesis_block) = self.fetch_genesis_block().await? {
			self.meta.send(genesis_block).await?;
		} else {
			log::error!("Couldn't fetch the genesis block from backend");
		}
		Ok(())
	}

	/// A async wrapper around the backend fn `iter_blocks` which
	/// runs in a `spawn_blocking` async task (its own thread)
	async fn collect_blocks(&self, fun: impl Fn(u32) -> bool + Send + 'static) -> Result<Vec<Block<B>>> {
		let backend = self.backend.clone();
		let now = std::time::Instant::now();
		let gather_blocks = move || -> Result<Vec<SignedBlock<B>>> {
			Ok(backend.iter_blocks(|n| fun(n))?.enumerate().map(|(_, b)| b).collect())
		};
		let blocks = smol::unblock(gather_blocks).await?;
		log::info!("Took {:?} to load {} blocks", now.elapsed(), blocks.len());
		let cache = self.rt_cache.clone();
		let blocks = smol::unblock(move || {
			// Finds the versions of all the blocks, returns a new set of type `Block`.
			// panics if our search fails to get the version for a block.
			cache.find_versions(&blocks).map(|versions| {
				blocks
					.into_iter()
					.map(|b| {
						let version =
							versions.iter().find(|&v| v.contains_block(b.block.header().number())).unwrap_or_else(
								|| panic!("Could not find a runtime version for block #{}", b.block.header().number()),
							);
						Block::new(b, version.version.spec_version)
					})
					.collect()
			})
		})
		.await?;
		Ok(blocks)
	}

	/// Collect blocks according to the predicate `fun` and send those blocks to
	///  the metadata actor.
	async fn collect_and_send(&self, fun: impl Fn(u32) -> bool + Send + 'static) -> Result<()> {
		self.meta.send(BatchBlock::new(self.collect_blocks(fun).await?)).await?;
		Ok(())
	}

	/// First run of indexing
	/// gets any blocks that are missing from database and indexes those.
	/// sets the `last_max` value.
	async fn re_index(&mut self) -> Result<()> {
		let mut conn = self.db.send(GetState::Conn.into()).await?.await?.conn();
		let cur_max = if let Some(m) = queries::max_block(&mut conn).await? {
			m
		} else {
			// a `None` means that the blocks table is not populated yet
			self.fetch_and_send_genesis_block().await?;
			log::info!("Fetch and send the genesis block");
			return Ok(());
		};

		let mut missing_blocks = 0;
		let mut min = self.last_max;
		loop {
			let batch = queries::missing_blocks_min_max(&mut conn, min, self.max_block_load).await?;
			if !batch.is_empty() {
				missing_blocks += batch.len();
				min += self.max_block_load;
				self.collect_and_send(move |n| batch.contains(&n)).await?;
			} else {
				break;
			}
		}

		self.last_max = cur_max;
		log::info!("{} missing blocks", missing_blocks);

		Ok(())
	}

	/// Crawl up to `max_block_load` blocks that are greater than the last max
	async fn crawl(&mut self) -> Result<Vec<Block<B>>> {
		let copied_last_max = self.last_max;
		let max_to_collect = copied_last_max + self.max_block_load;
		let blocks = self.collect_blocks(move |n| n > copied_last_max && n <= max_to_collect).await?;
		self.last_max = blocks
			.iter()
			.map(|b| (*b.inner.block.header().number()).into())
			.fold(self.last_max, |ac, e| if e > ac { e } else { ac });
		Ok(blocks)
	}
}

#[async_trait::async_trait]
impl<B: BlockT, D: ReadOnlyDB + 'static> Actor for BlocksIndexer<B, D>
where
	NumberFor<B>: Into<u32>,
	B: Unpin,
	B::Hash: Unpin,
{
	async fn started(&mut self, ctx: &mut Context<Self>) {
		// using this instead of notify_immediately because
		// ReIndexing is async process
		let addr = ctx.address().expect("Actor just started");

		addr.do_send(ReIndex).expect("Actor cannot be disconnected; just started");

		smol::spawn(async move {
			loop {
				if addr.send(Crawl).await.is_err() {
					break;
				}
			}
		})
		.detach();
	}
}

struct Crawl;
impl Message for Crawl {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin, D: ReadOnlyDB + 'static> Handler<Crawl> for BlocksIndexer<B, D>
where
	NumberFor<B>: Into<u32>,
	B::Hash: Unpin,
{
	async fn handle(&mut self, _: Crawl, ctx: &mut Context<Self>) {
		match self.crawl().await {
			Err(e) => log::error!("{}", e.to_string()),
			Ok(b) => {
				if !b.is_empty() && self.meta.send(BatchBlock::new(b)).await.is_err() {
					ctx.stop();
				}
			}
		}
	}
}

struct ReIndex;
impl Message for ReIndex {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin, D: ReadOnlyDB + 'static> Handler<ReIndex> for BlocksIndexer<B, D>
where
	NumberFor<B>: Into<u32>,
	B::Hash: Unpin,
{
	async fn handle(&mut self, _: ReIndex, ctx: &mut Context<Self>) {
		match self.re_index().await {
			// stop if disconnected from the metadata actor
			Err(ArchiveError::Disconnected) => ctx.stop(),
			Ok(()) => {}
			Err(e) => log::error!("{}", e.to_string()),
		}
	}
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin, D: ReadOnlyDB + 'static> Handler<Die> for BlocksIndexer<B, D>
where
	NumberFor<B>: Into<u32>,
	B::Hash: Unpin,
{
	async fn handle(&mut self, _: Die, ctx: &mut Context<Self>) {
		ctx.stop();
	}
}
