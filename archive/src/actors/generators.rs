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
    error::ArchiveResult, queries, sql_block_builder::BlockBuilder, threadpools::BlockData,
};
// use futures::{Stream, StreamExt};
use sp_runtime::traits::Block as BlockT;

/// Gets missing blocks from the SQL database
pub async fn missing_blocks(pool: sqlx::PgPool, sender: flume::Sender<u32>) -> ArchiveResult<()> {
    loop {
        // let conn = &mut pool.acquire().await?;
        let nums = queries::missing_blocks(&pool).await?;
        for num in nums.into_iter() {
            if let Err(_) = sender.send(num) {
                break;
            }
        }
        /*
        while let Some(Ok(num)) = stream.next().await {
            if let Err(_) = sender.send(num.0 as u32) {
                break;
            }
        }
        */
        timer::Delay::new(std::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Gets storage that is missing from the storage table
/// by querying it against the blocks table
/// This fills in storage that might've been missed by a shutdown
pub async fn fill_storage<B: BlockT>(
    pool: sqlx::PgPool,
    tx: flume::Sender<BlockData<B>>,
) -> ArchiveResult<()> {
    if queries::blocks_count(&pool).await? == 0 {
        // no blocks means we haven't indexed anything yet
        return Ok(());
    }
    let now = std::time::Instant::now();
    let blocks = queries::blocks_storage_intersection(&pool).await?;
    let blocks = BlockBuilder::<B>::new().with_vec(blocks)?;
    let elapsed = now.elapsed();
    log::info!(
        "TOOK {} seconds, {} milli-seconds to get and build {} blocks",
        elapsed.as_secs(),
        elapsed.as_millis(),
        blocks.len()
    );
    log::info!("indexing {} blocks of storage ... ", blocks.len());
    tx.send(BlockData::Batch(blocks))?;
    Ok(())
}
