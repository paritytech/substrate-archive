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
use futures::StreamExt;
use sp_runtime::traits::Block as BlockT;
use sqlx::{pool::PoolConnection, Postgres};
/// Gets missing blocks from the SQL database
pub async fn missing_blocks(
    mut conn: PoolConnection<Postgres>,
    sender: flume::Sender<u32>,
) -> ArchiveResult<()> {
    'gen: loop {
        let mut stream = queries::missing_blocks_stream(&mut conn);
        while let Some(num) = stream.next().await {
            match num {
                Ok(n) => {
                    // if this is an error the threadpool has disconnected and we can shutdown
                    if let Err(_) = sender.send(n.0 as u32) {
                        break 'gen;
                    }
                }
                Err(e) => {
                    // if an error occurs we should kill the loop
                    log::error!("{}", e.to_string());
                    break 'gen;
                }
            }
        }
        timer::Delay::new(std::time::Duration::from_secs(1)).await;
    }
    std::mem::drop(conn);
    Ok(())
}

/// Gets storage that is missing from the storage table
/// by querying it against the blocks table
/// This fills in storage that might've been missed by a shutdown
pub async fn fill_storage<B: BlockT>(
    mut conn: PoolConnection<Postgres>,
    tx: flume::Sender<BlockData<B>>,
) -> ArchiveResult<()> {
    if queries::blocks_count(&mut conn).await? == 0 {
        // no blocks means we haven't indexed anything yet
        return Ok(());
    }
    let now = std::time::Instant::now();
    let blocks = queries::blocks_storage_intersection(&mut conn).await?;
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
    std::mem::drop(conn);
    Ok(())
}
