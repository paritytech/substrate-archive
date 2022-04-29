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

use async_std::task;
use xtra::prelude::*;

use sp_runtime::{
	generic::SignedBlock,
	traits::{Block as BlockT, Header as _, NumberFor},
};
use substrate_archive_backend::{ReadOnlyBackend, ReadOnlyDb, RuntimeVersionCache};

use crate::{
	actors::{
		workers::{
			database::{DatabaseActor, GetState},
			metadata::MetadataActor,
		},
		SystemConfig,
	},
	database::queries,
	error::{ArchiveError, Result},
	types::{BatchBlock, Block},
};
type DatabaseAct = Address<DatabaseActor>;
type MetadataAct<B> = Address<MetadataActor<B>>;

pub struct BlocksIndexer<B: Send + 'static, D: Send + 'static> {
	/// background task to crawl blocks
	backend: Arc<ReadOnlyBackend<B, D>>,
	db: DatabaseAct,
	meta: MetadataAct<B>,
	rt_cache: Arc<RuntimeVersionCache<B, D>>,
	/// the last maximum block number from which we are sure every block before then is indexed
	last_max: u32,
	/// the maximum amount of blocks to index at once
	max_block_load: u32,
}

impl<B, D> BlocksIndexer<B, D>
where
	B: BlockT + Unpin,
	D: ReadOnlyDb + 'static,
	B::Hash: Unpin,
	NumberFor<B>: Into<u32>,
{
	pub fn new(conf: &SystemConfig<B, D>, db: DatabaseAct, meta: MetadataAct<B>) -> Self {
		Self {
			rt_cache: Arc::new(RuntimeVersionCache::new(conf.backend.clone(), conf.runtime.clone())),
			last_max: 0,
			backend: conf.backend().clone(),
			db,
			meta,
			max_block_load: conf.control.max_block_load,
		}
	}

	/// A async wrapper around the backend fn `iter_blocks` which
	/// runs in a `spawn_blocking` async task (its own thread)
	async fn collect_blocks(&self, fun: impl Fn(u32) -> bool + Send + 'static) -> Result<Vec<Block<B>>> {
		let now = std::time::Instant::now();
		let (backend, cache) = (self.backend.clone(), self.rt_cache.clone());
		let blocks = task::spawn_blocking(move || {
			let blocks: Vec<SignedBlock<B>> = backend.iter_blocks(fun)?.collect();
			if !blocks.is_empty() {
				log::info!("Took {:?} to load {} blocks", now.elapsed(), blocks.len());
			} else {
				return Ok(Vec::new());
			}
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
		let mut conn = self.db.send(GetState::Conn).await??.conn();
		let cur_max = if let Some(m) = queries::max_block(&mut conn).await? {
			m
		} else {
			// a `None` means that the blocks table is not populated yet
			log::info!("{} missing blocks", 0);
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
		log::info!("{} missing blocks, max currently indexed {}", missing_blocks, cur_max);

		Ok(())
	}

	/// Crawl up to `max_block_load` blocks that are greater than the last max
	async fn crawl(&mut self) -> Result<Vec<Block<B>>> {
		let copied_last_max = self.last_max;
		let max_to_collect = copied_last_max + self.max_block_load;
		let blocks = self
			.collect_blocks(move |n| {
				if copied_last_max == 0 {
					// includes the genesis block
					n >= copied_last_max && n <= max_to_collect
				} else {
					n > copied_last_max && n <= max_to_collect
				}
			})
			.await?;
		self.last_max = blocks
			.iter()
			.map(|b| (*b.inner.block.header().number()).into())
			.fold(self.last_max, |ac, e| if e > ac { e } else { ac });
		Ok(blocks)
	}
}

#[async_trait::async_trait]
impl<B: Send + Sync, D: Send + Sync> Actor for BlocksIndexer<B, D> {}

pub struct Crawl;
impl Message for Crawl {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin, D: ReadOnlyDb + 'static> Handler<Crawl> for BlocksIndexer<B, D>
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

pub struct ReIndex;
impl Message for ReIndex {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin, D: ReadOnlyDb + 'static> Handler<ReIndex> for BlocksIndexer<B, D>
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
