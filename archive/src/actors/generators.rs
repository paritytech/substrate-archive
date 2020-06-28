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
use crate::{error::ArchiveResult, queries};
use futures::future::Future;
use hashbrown::HashSet;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use xtra::prelude::*;
mod blocks;
mod missing_storage;

//pub use self::missing_blocks::block_loop;
pub use blocks::BlocksActor;
pub use missing_storage::MissingStorage;
pub mod msg {
    pub use super::blocks::Head;
}

/// Gets missing blocks from the SQL database
pub async fn missing_blocks<B>(context: ActorContext<B>, addr: Address<BlockFetcher<B>>) -> ArchiveResult<()>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    let mut added = HashSet::new();
    loop {
        match main_l(&context.pool(), &addr, &mut added).await {
            Ok(_) => (),
            Err(e) => log::error!("{}", e),
        }
    }
}

// TODO: once async closures are stabilized this could be a closure apart of the missing_blocks fn
async fn main_l<B>(
    pool: &sqlx::PgPool,
    addr: &Address<BlockFetcher<B>>,
    added: &mut HashSet<u32>,
) -> ArchiveResult<()>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    let block_nums = queries::missing_blocks(pool).await?;
    if block_nums.len() <= 0 {
        timer::Delay::new(std::time::Duration::from_secs(1)).await;
        return Ok(());
    }
    let block_nums = block_nums
        .into_iter()
        .map(|b| b.generate_series as u32)
        .collect::<HashSet<u32>>();

    let block_nums = block_nums.difference(&added).map(|b| *b).collect::<Vec<u32>>();
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
    Ok(())
}
