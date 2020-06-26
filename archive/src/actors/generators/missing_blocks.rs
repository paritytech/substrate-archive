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

//! Work generated and gathered from the PostgreSQL Database
//! IE: Missing Blocks/Storage/Inherents/Transactions
//! Gathers Missing blocks -> passes to metadata -> passes to extractors -> passes to decode -> passes to insert

use crate::actors::{
    scheduler::{Algorithm, Scheduler},
    workers, ActorContext,
};
use crate::{
    backend::BlockData,
    error::Error as ArchiveError,
    print_on_err, queries,
    types::{NotSignedBlock, Substrate, SubstrateBlock, System},
};
use bastion::prelude::*;
use sp_runtime::generic::BlockId;

pub fn actor<T>(context: ActorContext<T>) -> Result<ChildrenRef, ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
    <T as System>::Header: serde::de::DeserializeOwned,
{
    // let meta_workers = workers::metadata::<T>(context.rpc_url().to_string(), context.pool().clone())?;
    // generate work from missing blocks
    Bastion::children(|children| {
        children.with_exec(move |ctx: BastionContext| {
            let context = context.clone();
            // let workers = meta_workers.clone();
            async move {
                let mut sched = Scheduler::new(Algorithm::RoundRobin, &ctx);
                // sched.add_worker("meta", &workers);
                loop {
                    if handle_shutdown(&ctx).await {
                        break;
                    }
                    print_on_err!(entry::<T>(&context, &mut sched).await);
                }
                Bastion::stop();
                Ok(())
            }
        })
    })
    .map_err(|_| ArchiveError::from("Could not instantiate database generator"))
}

async fn entry<T>(context: &ActorContext<T>, sched: &mut Scheduler<'_>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    NotSignedBlock<T>: serde::Serialize + serde::de::DeserializeOwned,
{
    let block_nums = queries::missing_blocks(&context.pool()).await?;
    log::info!("missing {} blocks", block_nums.len());
    if !(block_nums.len() > 0) {
        timer::Delay::new(std::time::Duration::from_secs(5)).await;
        return Ok(());
    }
    log::info!(
        "Indexing {} missing blocks, from {} to {} ...",
        block_nums.len(),
        block_nums[0].generate_series,
        block_nums[block_nums.len() - 1].generate_series
    );
    let backend = context.backend().clone();
    let broker = context.broker().clone();
    let now = std::time::Instant::now();
    let blocks: Result<Vec<SubstrateBlock<T>>, ArchiveError> = blocking!((move || {
        let mut blocks = Vec::new();
        for block_num in block_nums.iter() {
            let num = block_num.generate_series as u32;
            let b = backend.block(&BlockId::Number(T::BlockNumber::from(num)));

            if b.is_none() {
                log::warn!("Block does not exist!");
            } else {
                let b = b.expect("Checked for none; qed");
                broker.work.send(BlockData::Single(b.block.clone()))?;
                blocks.push(b);
            }
        }
        Ok(blocks)
    })())
    .await
    .unwrap();

    let blocks = blocks?;

    let elapsed = now.elapsed();
    log::info!(
        "Took {} seconds to crawl {} missing blocks",
        elapsed.as_secs(),
        blocks.len()
    );
    let answer = sched.ask_next("meta", blocks)?.await;
    log::debug!("{:?}", answer);
    Ok(())
}

// Handle a shutdown
async fn handle_shutdown(ctx: &BastionContext) -> bool {
    if let Some(msg) = ctx.try_recv().await {
        msg! {
            msg,
            broadcast: super::Broadcast => {
                match broadcast {
                    super::Broadcast::Shutdown => {
                        return true;
                    }
                }
            };
            e: _ => log::warn!("Received unknown message: {:?}", e);
        };
    }
    false
}
