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

use crate::actors::{workers::BlockFetcher, ActorContext};
use crate::{backend::BlockData, error::ArchiveResult};
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use jsonrpsee::client::Subscription;
use serde::de::DeserializeOwned;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use xtra::prelude::*;

pub struct BlocksActor<Block: BlockT> {
    /// Url to the JSONRPC interface of a running substrate node
    url: String,
    fetch: Address<BlockFetcher<Block>>,
    stream_handle: tokio::task::JoinHandle<ArchiveResult<()>>,
    tx: Sender<()>,
}

impl<Block: BlockT> Actor for BlocksActor<Block> {}

impl<Block> BlocksActor<Block>
where
    Block: BlockT,
    Block::Header: DeserializeOwned,
    NumberFor<Block>: Into<u32>,
{
    pub fn new(url: String, addr: Address<BlockFetcher<Block>>) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(0);
        let stream_handle = tokio::spawn(blocks_stream(url.clone(), addr.clone(), rx));

        Self {
            url,
            fetch: addr,
            stream_handle,
            tx,
        }
    }

    pub async fn stop(self) -> ArchiveResult<()> {
        self.tx.send(())?;
        self.stream_handle.await.expect("Join should be infallible");
        Ok(())
    }
}

pub struct Head<Block: BlockT>(pub Block::Header);

impl<Block: BlockT> Message for Head<Block> {
    type Result = ArchiveResult<()>;
}

pub async fn blocks_stream<Block>(
    url: String,
    addr: Address<BlockFetcher<Block>>,
    rx: Receiver<()>,
) -> ArchiveResult<()>
where
    Block: BlockT,
    Block::Header: DeserializeOwned,
    NumberFor<Block>: Into<u32>,
{
    let rpc = crate::rpc::Rpc::<Block>::connect(url.as_str()).await?;
    let mut subscription = rpc.subscribe_finalized_heads().await?;
    loop {
        let head = subscription.next().await;
        addr.send(Head::<Block>(head));
        match rx.try_recv() {
            Ok(_) => break,
            Err(_) => continue,
        }
    }
    Ok(())
}
