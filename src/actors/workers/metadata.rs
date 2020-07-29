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

use super::{database::GetState, ActorPool};
use crate::{
    backend::Meta,
    database::DbConn,
    error::ArchiveResult,
    queries,
    types::{BatchBlock, Block, Metadata as MetadataT},
};
use itertools::Itertools;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header as _, NumberFor},
};
use xtra::prelude::*;

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
pub struct Metadata<B: BlockT> {
    conn: DbConn,
    addr: Address<ActorPool<super::DatabaseActor<B>>>,
    meta: Meta<B>,
}

impl<B: BlockT> Metadata<B> {
    pub async fn new(
        addr: Address<ActorPool<super::DatabaseActor<B>>>,
        meta: Meta<B>,
    ) -> ArchiveResult<Self> {
        let conn = addr.send(GetState::Conn.into()).await?.await?.conn();
        Ok(Self { conn, addr, meta })
    }

    // checks if the metadata exists in the database
    // if it doesn't exist yet, fetch metadata and insert it
    async fn meta_checker(&mut self, ver: u32, hash: B::Hash) -> ArchiveResult<()> {
        if !queries::check_if_meta_exists(ver, &mut self.conn).await? {
            log::info!("META DONT EXIST BOI {}", ver);
            let meta = self.meta.clone();
            log::info!("Getting metadata for hash {}", hex::encode(hash.as_ref()));
            let meta = smol::unblock!(meta.metadata(&BlockId::hash(hash)))?;
            log::info!("Got metadata for version {}", ver);
            let meta: sp_core::Bytes = meta.into();
            let meta = MetadataT::new(ver, meta.0);
            self.addr.send(meta.into()).await?;
        }
        Ok(())
    }

    async fn block_handler(&mut self, blk: Block<B>) -> ArchiveResult<()>
    where
        NumberFor<B>: Into<u32>,
    {
        let hash = blk.inner.block.header().hash();
        self.meta_checker(blk.spec, hash).await?;
        self.addr.do_send(blk.into())?;
        Ok(())
    }

    async fn batch_block_handler(&mut self, blks: BatchBlock<B>) -> ArchiveResult<()>
    where
        NumberFor<B>: Into<u32>,
    {
        let versions = blks
            .inner()
            .iter()
            .unique_by(|b| b.spec)
            .collect::<Vec<&Block<B>>>();
        log::info!("GETTING METADATA for {} versions", versions.len());
        for b in versions.iter() {
            log::info!(
                "Getting metadata for block {}, version: {}",
                b.inner.block.header().number(),
                b.spec
            );
            self.meta_checker(b.spec, b.inner.block.hash()).await?;
        }
        log::info!("GOT METADATA");
        let len = blks.inner().len();
        self.addr.send(blks.into()).await?;
        log::info!("Sent {} blocks", len);
        Ok(())
    }
}

impl<B: BlockT> Actor for Metadata<B> {}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for Metadata<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blk: Block<B>, _: &mut Context<Self>) {
        if let Err(e) = self.block_handler(blk).await {
            log::error!("{}", e.to_string());
        }
    }
}

#[async_trait::async_trait]
impl<B> Handler<BatchBlock<B>> for Metadata<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blks: BatchBlock<B>, _: &mut Context<Self>) {
        if let Err(e) = self.batch_block_handler(blks).await {
            log::error!("{}", e.to_string());
        }
    }
}
