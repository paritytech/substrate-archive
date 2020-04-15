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

use futures::{
    channel::mpsc::{self, Sender, Receiver},
    future::{join, join_all},
    TryFutureExt, StreamExt, Stream,
    executor::block_on,
};
use runtime_primitives::traits::{Block as _, Header as HeaderTrait};
//use substrate_primitives::storage::StorageKey;
use desub::decoder::Metadata;
use runtime_version::RuntimeVersion;
use substrate_rpc_primitives::number::NumberOrHex;
use subxt::Client;

use std::{sync::Arc, thread::{self, JoinHandle}};

use crate::{
    error::Error as ArchiveError,
    types::{BatchBlockItem, BatchData, Block, Data, Header, Storage, Substrate, SubstrateBlock},
};

/// Communicate with Substrate node via RPC
#[derive(Clone)]
pub struct Rpc<T: Substrate + Send + Sync> {
    client: Client<T>,
    // keys: Vec<StorageKey>,
    // properties: Properties,
}

/// Methods that fetch a value from RPC and send to sender
impl<T> Rpc<T>
where
    T: Substrate + Send + Sync,
{
    pub async fn subscribe_finalized_heads(&self) -> Result<jsonrpsee::client::Subscription<T::Header>, ArchiveError> {
        self.client.subscribe_finalized_blocks().await?;
    }
    
    /// subscribes to new heads but sends blocks instead of headers
    /// spawns a background task to collect blocks, sending them back to the main thread
    pub fn subscribe_blocks(
        self: Arc<Self>,
    ) -> Result<(impl StreamExt<Item = Block<T>>, JoinHandle<Result<(), ArchiveError>>), ArchiveError> {
        let (tx, rx) = mpsc::channel(16); // with a limit of 16 blocks in the queue at once
        let rpc = self.clone();
        let thread = thread::Builder::new().name("Substrate Block Collector".to_string()); 
        let handle = thread.spawn(move || {
            block_on(self.subscribe_finalized_blocks(tx))
        }).unwrap();
        Ok((rx, handle))
    }

    /// send all finalized headers back to main thread
    pub async fn subscribe_finalized_blocks(
        &self,
        mut sender: Sender<Block<T>>,
    ) -> Result<(), ArchiveError> {
        let mut stream = self.client.subscribe_finalized_blocks().await?;
        loop {
            log::info!("Awaiting next head...");
            let head = stream.next().await;
            log::info!("Converting to block...");
            let block = self.block(Some(head.hash())).await?;
            log::info!("Received a new block!");
            if let Some(b) = block {
                sender
                    .try_send(Block::new(b))
                    .map_err(|e| ArchiveError::from(e))?;
            } else {
                log::warn!("Block does not exist");
            }
        }
    }
}

/// Methods that return fetched value directly
impl<T> Rpc<T>
where
    T: Substrate + Send + Sync,
{
    pub fn new(client: subxt::Client<T>) -> Self {
        Self { client }
    }

    /// Fetch a block by hash from Substrate RPC
    pub(crate) async fn block(
        &self,
        hash: Option<T::Hash>,
    ) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        self.client.block(hash).await.map_err(Into::into)
    }

    /// get the latest block
    pub(crate) async fn latest_block(&self) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        self.client.block::<T::Hash>(None).await.map_err(Into::into)
    }

    /// get just the latest header
    pub(crate) async fn latest_head(&self) -> Result<Option<T::Header>, ArchiveError> {
        self.client
            .header::<T::Hash>(None)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn version(
        &self,
        hash: Option<&T::Hash>,
    ) -> Result<RuntimeVersion, ArchiveError> {
        self.client
            .runtime_version(hash)
            .map_err(Into::into)
            .await
    }

    pub(crate) async fn metadata(&self, hash: Option<&T::Hash>) -> Result<Metadata, ArchiveError> {
        let meta = self
            .client
            .raw_metadata(hash)
            .map_err(ArchiveError::from)
            .await?;
        Ok(Metadata::new(meta.as_slice()))
    }

    pub(crate) async fn meta_and_version(&self, hash: Option<T::Hash>) -> Result<(RuntimeVersion, Metadata), ArchiveError> {
        let meta = self.client.raw_metadata(hash.as_ref())
                            .map_err(ArchiveError::from);
        let version = self.client.runtime_version(hash.as_ref())
                                .map_err(ArchiveError::from);
        let (meta, version) = join(meta, version).await;
        let meta = meta?;
        let version = version?;
        Ok((version, Metadata::new(meta.as_slice())))
    }

    pub async fn block_from_number(
        &self,
        number: NumberOrHex<T::BlockNumber>,
    ) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        let hash = self.client.block_hash(Some(number.into())).await?;
        self.client.block(hash).await.map_err(Into::into)
    }

    /// get a batch of blocks, with metadata and runtime version
    pub async fn batch_block_from_number(
        &self,
        numbers: Vec<NumberOrHex<T::BlockNumber>>,
    ) -> Result<Vec<BatchBlockItem<T>>, ArchiveError> {
        let mut blocks = Vec::new();
        for num in numbers.into_iter() {
            let block = self.block_from_number(num);
            blocks.push(block);
        }

        let mut meta_futures = Vec::new();

        let blocks: Vec<_> = join_all(blocks)
                .await
                .into_iter()
                .map(|b| b.transpose())
                // ignore blocks that don't exist
                .filter_map(|b| b)
                .collect::<Result<Vec<_>, _>>()?;

        for b in blocks.iter() {
            meta_futures.push(self.meta_and_version(Some((b.block.header().hash()).clone())));
        }

        let meta_futures = join_all(meta_futures).await.into_iter().collect::<Result<Vec<_>, _>>()?;
        let mut batch_items = Vec::new();
        for (b, m) in blocks.into_iter().zip(meta_futures.into_iter()) {
            batch_items.push(BatchBlockItem::<T>::new(b, m.1, m.0.spec_version));
        }
        Ok(Vec::new())
    }

    /// unsubscribe from finalized heads
    #[allow(dead_code)]
    fn unsubscribe_finalized_heads() {
        unimplemented!();
    }

    /// unsubscribe from new heads
    #[allow(dead_code)]
    fn unsubscribe_new_heads() {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn can_query_blocks() {}
}
