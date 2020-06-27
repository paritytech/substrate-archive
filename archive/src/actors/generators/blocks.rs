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

//! Block actor
//! Gets new finalized blocks from substrate RPC

use crate::actors::{workers, ActorContext};
use crate::{backend::BlockData, error::ArchiveResult};
use async_trait::async_trait;
use jsonrpsee::client::Subscription;
use serde::de::DeserializeOwned;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::{Block as BlockT, Header as _};
use xtra::prelude::*;

pub struct BlocksActor<Block: BlockT> {
    context: ActorContext<Block>,
    metadata: Address<workers::Metadata>,
}
impl<Block: BlockT> Actor for BlocksActor<Block> {}

impl<Block: BlockT> BlocksActor<Block> {
    pub fn new(context: ActorContext<Block>) -> Self {
        let addr = workers::Metadata::new(context.rpc_url().to_string(), context.pool()).spawn();
        Self {
            context,
            metadata: addr,
        }
    }
}

pub struct Head<Block: BlockT>(pub Block::Header);

impl<Block: BlockT> Message for Head<Block> {
    type Result = ArchiveResult<()>;
}

#[async_trait]
impl<Block: BlockT> Handler<Head<Block>> for BlocksActor<Block> {
    async fn handle(&mut self, head: Head<Block>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let block = self.context.backend().block(&BlockId::Number(*head.0.number()));
        if let Some(b) = block {
            log::trace!("{:?}", b);
            self.context
                .broker
                .work
                .send(BlockData::Single(b.block.clone()))?;
        // self.metadata.send(BlockMsg::<T>::from(b));
        } else {
            log::warn!("Block does not exist");
        }
        Ok(())
    }
}

// TODO: can be spawned in 'actor started' method
pub async fn blocks_stream<Block>(context: ActorContext<Block>) -> ArchiveResult<()>
where
    Block: BlockT,
    Block::Header: DeserializeOwned,
{
    let addr = BlocksActor::new(context.clone()).spawn();
    let rpc = crate::rpc::Rpc::<Block>::connect(context.rpc_url()).await?;
    let mut subscription = rpc.subscribe_finalized_heads().await?;
    loop {
        let head = subscription.next().await;
        addr.send(Head::<Block>(head));
    }
}
