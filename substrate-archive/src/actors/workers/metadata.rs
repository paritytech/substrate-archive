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

use itertools::Itertools;
use xtra::prelude::*;

use sp_runtime::{
	generic::BlockId,
	traits::{Block as BlockT, NumberFor},
};
use substrate_archive_backend::Meta;

use crate::{
	actors::workers::database::{DatabaseActor, GetState},
	database::{queries, DbConn},
	error::Result,
	types::{BatchBlock, Block, Die, Metadata},
};

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
pub struct MetadataActor<B: Send + 'static> {
	conn: DbConn,
	addr: Address<DatabaseActor>,
	meta: Meta<B>,
}

impl<B: BlockT + Unpin> MetadataActor<B> {
	pub async fn new(addr: Address<DatabaseActor>, meta: Meta<B>) -> Result<Self> {
		let conn = addr.send(GetState::Conn).await??.conn();
		Ok(Self { conn, addr, meta })
	}

	// checks if the metadata exists in the database
	// if it doesn't exist yet, fetch metadata and insert it
	async fn meta_checker(&mut self, ver: u32, hash: B::Hash) -> Result<()> {
		if !queries::check_if_meta_exists(ver, &mut self.conn).await? {
			let meta = self.meta.clone();
			log::info!("Getting metadata for hash {}, version {}", hex::encode(hash.as_ref()), ver);
			let meta = smol::unblock(move || meta.metadata(&BlockId::hash(hash))).await?;
			let meta = Metadata::new(ver, meta.to_vec());
			self.addr.send(meta).await?;
		}
		Ok(())
	}

	async fn block_handler(&mut self, blk: Block<B>) -> Result<()>
	where
		NumberFor<B>: Into<u32>,
	{
		let hash = blk.inner.block.hash();
		self.meta_checker(blk.spec, hash).await?;
		self.addr.send(blk).await?;
		Ok(())
	}

	async fn batch_block_handler(&mut self, blks: BatchBlock<B>) -> Result<()>
	where
		NumberFor<B>: Into<u32>,
	{
		for blk in blks.inner().iter().unique_by(|&blk| blk.spec) {
			self.meta_checker(blk.spec, blk.inner.block.hash()).await?;
		}
		self.addr.send(blks).await?;
		Ok(())
	}
}

impl<B: Send> Actor for MetadataActor<B> {}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for MetadataActor<B>
where
	B: BlockT + Unpin,
	NumberFor<B>: Into<u32>,
{
	async fn handle(&mut self, blk: Block<B>, _: &mut Context<Self>) {
		if let Err(e) = self.block_handler(blk).await {
			log::error!("{}", e.to_string());
		}
	}
}

#[async_trait::async_trait]
impl<B> Handler<BatchBlock<B>> for MetadataActor<B>
where
	B: BlockT + Unpin,
	NumberFor<B>: Into<u32>,
{
	async fn handle(&mut self, blks: BatchBlock<B>, _: &mut Context<Self>) {
		if let Err(e) = self.batch_block_handler(blks).await {
			log::error!("{}", e.to_string());
		}
	}
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<Die> for MetadataActor<B>
where
	NumberFor<B>: Into<u32>,
	B::Hash: Unpin,
{
	async fn handle(&mut self, _: Die, ctx: &mut Context<Self>) {
		ctx.stop();
	}
}
