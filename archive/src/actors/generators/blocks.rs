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

use crate::actors::{self, workers, ActorContext};
use crate::{
    backend::BlockData,
    error::Error as ArchiveError,
    types::{Substrate, System},
};
use async_trait::async_trait;
// use jsonrpsee::client::Subscription;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::Header as _;
use xtra::prelude::*;

struct BlocksActor<T: Substrate + Send + Sync> {
    context: ActorContext<T>,
}

struct Head<T: Substrate + Send + Sync>(T::Header);

impl<T> Message for Head<T>
where
    T: Substrate + Send + Sync,
{
    type Result = Result<(), ArchiveError>;
}

impl<T> Actor for BlocksActor<T> where T: Substrate + Send + Sync {}

#[async_trait]
impl<T> Handler<Head<T>> for BlocksActor<T>
where
    T: Substrate + Send + Sync,
{
    async fn handle(&mut self, head: Head<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        let block = self.context.backend().block(&BlockId::Number(*head.0.number()));
        if let Some(b) = block {
            log::trace!("{:?}", b);
            let block = b.block.clone();
            self.context.broker.work.send(BlockData::Single(block))?;
        // sched.tell_next("meta", b)?
        } else {
            log::warn!("Block does not exist");
        }
        Ok(())
    }
}
