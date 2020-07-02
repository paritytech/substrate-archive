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

use super::workers::msg::BlockRange;
use super::{workers::BlockFetcher, ActorContext};
use crate::{
    backend::{BlockBroker, BlockData},
    error::ArchiveResult,
    queries,
    sql_block_builder::BlockBuilder,
};
use futures::future::Future;
use hashbrown::HashSet;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use xtra::prelude::*;

/// Gets missing blocks from the SQL database
pub async fn missing_blocks<B>(
    pool: sqlx::PgPool,
    addr: Address<BlockFetcher<B>>,
) -> ArchiveResult<()>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    let mut added = HashSet::new();
    loop {
        let block_nums = queries::missing_blocks(&pool).await?;
        if block_nums.len() <= 0 {
            timer::Delay::new(std::time::Duration::from_secs(1)).await;
            return Ok(());
        }
        let block_nums = block_nums
            .into_iter()
            .map(|b| b.generate_series as u32)
            .collect::<HashSet<u32>>();
        let block_nums = block_nums
            .difference(&added)
            .map(|b| *b)
            .collect::<Vec<u32>>();
        if block_nums.len() > 0 {
            log::info!(
                "Indexing {} missing blocks, from {} to {} ...",
                block_nums.len(),
                block_nums[0],
                block_nums[block_nums.len() - 1]
            );
            added.extend(block_nums.iter());
            addr.do_send(BlockRange(block_nums));
            timer::Delay::new(std::time::Duration::from_secs(1)).await;
        } else {
            timer::Delay::new(std::time::Duration::from_secs(5)).await;
        }
    }
    Ok(())
}

/// Gets storage that is missing from the storage table
/// by querying it against the blocks table
/// This fills in storage that might've been missed by a shutdown
pub async fn fill_storage<B: BlockT>(
    pool: sqlx::PgPool,
    broker: BlockBroker<B>,
) -> ArchiveResult<()> {
    if queries::blocks_count(&pool).await? <= 0 {
        // no blocks means we haven't indexed anything yet
        return Ok(());
    }
    let now = std::time::Instant::now();
    let blocks = queries::blocks_storage_intersection(&pool).await?;
    let blocks = BlockBuilder::new().with_vec(blocks)?;
    let elapsed = now.elapsed();
    log::info!(
        "TOOK {} seconds, {} milli-seconds to get and build {} blocks",
        elapsed.as_secs(),
        elapsed.as_millis(),
        blocks.len()
    );
    log::info!("indexing {} blocks of storage ... ", blocks.len());
    broker.work.send(BlockData::Batch(blocks));
    Ok(())
}
