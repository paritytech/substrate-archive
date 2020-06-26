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
    workers::{self, msg::BlocksMsg},
    ActorContext,
};
use crate::{
    backend::BlockData,
    error::Error as ArchiveError,
    queries,
    types::{NotSignedBlock, Substrate, SubstrateBlock, System},
};
use sp_runtime::generic::BlockId;
use xtra::prelude::*;

pub fn block_loop<T>(context: ActorContext<T>, handle: tokio::runtime::Handle) -> std::thread::JoinHandle<()>
where
    T: Substrate + Send + Sync,
{
    std::thread::spawn(move || loop {
        let addr =
            handle.enter(|| workers::Metadata::new(context.rpc_url().to_string(), context.pool()).spawn());

        match generator(context.clone(), addr) {
            Ok(_) => (),
            Err(e) => {
                log::error!("{:?}", e);
            }
        }
    })
}

fn generator<T>(context: ActorContext<T>, addr: Address<workers::Metadata>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    let block_nums = futures::executor::block_on(queries::missing_blocks(&context.pool()))?;
    log::info!("missing {} blocks", block_nums.len());
    if !(block_nums.len() > 0) {
        std::thread::sleep(std::time::Duration::from_secs(5));
        return Ok(());
    }
    log::info!(
        "Indexing {} missing blocks, from {} to {} ...",
        block_nums.len(),
        block_nums[0].generate_series,
        block_nums[block_nums.len() - 1].generate_series
    );
    let mut blocks = Vec::new();
    let now = std::time::Instant::now();
    for block_num in block_nums.iter() {
        let num = block_num.generate_series as u32;
        let b = context
            .backend()
            .block(&BlockId::Number(T::BlockNumber::from(num)));
        if b.is_none() {
            log::warn!("Block does not exist!");
        } else {
            let b = b.expect("Checked for none; qed");
            context.broker().work.send(BlockData::Single(b.block.clone()))?;
            blocks.push(b);
        }
    }
    let elapsed = now.elapsed();
    log::info!(
        "Took {} seconds to crawl {} missing blocks",
        elapsed.as_secs(),
        blocks.len()
    );
    let res = addr.send(BlocksMsg::<T>::from(blocks));
    let answer = futures::executor::block_on(res);
    log::debug!("{:?}", answer);
    Ok(())
}
