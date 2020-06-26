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
    error::Error as ArchiveError,
    queries,
    rpc::Rpc,
    types::{Block, Metadata, Substrate, SubstrateBlock},
};
use serde::de::DeserializeOwned;
use sp_runtime::traits::{Block as _, Header as _};
use xtra::prelude::*;

/// Actor to fetch metadata about a block/blocks from RPC
/// Accepts workers to decode blocks and a URL for the RPC
struct MetadataActor {
    url: String,
    pool: sqlx::PgPool,
}

impl Actor for MetadataActor {}

#[derive(Debug)]
struct BlockMsg<T: Substrate>(SubstrateBlock<T>);

#[derive(Debug)]
struct BlocksMsg<T: Substrate>(Vec<SubstrateBlock<T>>);

impl<T: Substrate> Message for BlockMsg<T> {
    type Result = Result<(), ArchiveError>;
}

impl<T: Substrate> Message for BlocksMsg<T> {
    type Result = Result<(), ArchiveError>;
}

#[async_trait::async_trait]
impl<T> Handler<BlockMsg<T>> for MetadataActor
where
    T: Substrate,
{
    async fn handle(&mut self, blk: BlockMsg<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        let rpc = super::connect::<T>(self.url.as_str()).await;
        meta_process_block::<T>(blk.0, rpc, &self.pool).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Handler<BlocksMsg<T>> for MetadataActor
where
    T: Substrate,
{
    async fn handle(&mut self, blk: BlocksMsg<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        let rpc = super::connect::<T>(self.url.as_str()).await;
        meta_process_blocks::<T>(blk.0, rpc, &self.pool).await?;
        Ok(())
    }
}

async fn meta_process_block<T>(
    block: SubstrateBlock<T>,
    rpc: Rpc<T>,
    pool: &sqlx::PgPool,
    // sched: &mut Scheduler<'_>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    let hash = block.block.header().hash();
    let ver = rpc.version(Some(hash).as_ref()).await?;
    meta_checker(ver.spec_version, Some(hash), &rpc, pool).await?;
    let block = Block::<T>::new(block, ver.spec_version);
    // let v = sched.ask_next("transform", block)?.await;
    // log::debug!("{:?}", v);
    Ok(())
}

async fn meta_process_blocks<T>(
    blocks: Vec<SubstrateBlock<T>>,
    rpc: Rpc<T>,
    pool: &sqlx::PgPool,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    log::info!("Got {} blocks", blocks.len());
    let mut batch_items = Vec::new();

    let now = std::time::Instant::now();
    let first = rpc.version(Some(&blocks[0].block.header().hash())).await?;
    let elapsed = now.elapsed();
    log::debug!("Rpc request for version took {} milli-seconds", elapsed.as_millis());
    let last = rpc
        .version(Some(blocks[blocks.len() - 1].block.header().hash()).as_ref())
        .await?;
    log::info!(
        "First Version: {}, Last Version: {}",
        first.spec_version,
        last.spec_version
    );
    // if first and last versions of metadata are the same, we only need to do one check
    if first == last {
        meta_checker(first.spec_version, Some(blocks[0].block.header().hash()), &rpc, pool).await?;
        blocks
            .into_iter()
            .for_each(|b| batch_items.push(Block::<T>::new(b, first.spec_version)));
    } else {
        for b in blocks.into_iter() {
            let hash = b.block.header().hash();
            let ver = rpc.version(Some(hash).as_ref()).await?;
            meta_checker(ver.spec_version, Some(hash), &rpc, pool).await?;
            batch_items.push(Block::<T>::new(b, ver.spec_version))
        }
    }

    // let v = sched.ask_next("transform", batch_items)?.await;
    // log::debug!("{:?}", v);
    Ok(())
}

// checks if the metadata exists in the database
// if it doesn't exist yet, fetch metadata and insert it
async fn meta_checker<T>(ver: u32, hash: Option<T::Hash>, rpc: &Rpc<T>, pool: &sqlx::PgPool) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    if !queries::check_if_meta_exists(ver, pool).await? {
        let meta = rpc.metadata(hash).await?;
        let meta = Metadata::new(ver, meta);
        // let v = sched.ask_next("transform", meta)?.await;
        // log::debug!("{:?}", v);
    }
    Ok(())
}
