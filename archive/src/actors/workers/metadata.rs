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

use crate::{
    error::ArchiveResult,
    queries,
    rpc::Rpc,
    types::{BatchBlock, Block, Metadata as MetadataT},
};
use itertools::Itertools;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use xtra::prelude::*;

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
pub struct Metadata {
    url: String,
    pool: sqlx::PgPool,
    addr: Address<super::Database>,
}

impl Metadata {
    pub fn new(url: String, pool: &sqlx::PgPool) -> Self {
        let addr = super::Database::new(pool).spawn();
        Self {
            url,
            pool: pool.clone(),
            addr,
        }
    }

    // checks if the metadata exists in the database
    // if it doesn't exist yet, fetch metadata and insert it
    async fn meta_checker<B: BlockT>(
        &self,
        ver: u32,
        hash: B::Hash,
        rpc: &Rpc<B>,
    ) -> ArchiveResult<()> {
        if !queries::check_if_meta_exists(ver, &self.pool).await? {
            let meta = rpc.metadata(Some(hash)).await?;
            let meta = MetadataT::new(ver, meta);
            self.addr.do_send(meta)?;
        }
        Ok(())
    }
}

impl Actor for Metadata {}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for Metadata
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blk: Block<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let rpc = super::connect::<B>(self.url.as_str()).await;
        let hash = blk.inner.block.header().hash();
        self.meta_checker(blk.spec, hash, &rpc).await?;
        self.addr.do_send(blk)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B> Handler<BatchBlock<B>> for Metadata
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blks: BatchBlock<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let rpc = super::connect::<B>(self.url.as_str()).await;

        let versions = blks
            .inner()
            .iter()
            .unique_by(|b| b.spec)
            .collect::<Vec<&Block<B>>>();

        for b in versions.iter() {
            self.meta_checker(b.spec, b.inner.block.hash(), &rpc)
                .await?;
        }
        self.addr.do_send(blks)?;
        Ok(())
    }
}
