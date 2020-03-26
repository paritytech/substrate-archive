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

use futures::{TryFutureExt, channel::mpsc::UnboundedSender};
use runtime_primitives::traits::Header as HeaderTrait;
//use substrate_primitives::storage::StorageKey;
use runtime_version::RuntimeVersion;
use substrate_rpc_primitives::number::NumberOrHex;
use subxt::Client;

use std::sync::Arc;

use crate::{
    error::Error as ArchiveError,
    types::{BatchBlock, Block, Data, BatchData, Header, Storage, SubstrateBlock, Substrate},
};

/// Communicate with Substrate node via RPC
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
    /// subscribes to new heads but sends blocks instead of headers
    pub async fn subscribe_blocks(
        self: Arc<Self>,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let mut stream = self.client.subscribe_finalized_blocks().await?;
        while let head = stream.next().await {
            log::info!("Got Head: {:?}", head);
            self.clone()
                .block(Some(head.hash()), sender.clone())
                .await?;
        }
        Ok(())
    }
    
    /// send all new headers back to main thread
    pub async fn subscribe_new_heads(
        &self,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let mut stream = self.client.subscribe_blocks().await?;

        while let head = stream.next().await {
            sender
                .unbounded_send(Data::Header(Header::new(head)))
                .map_err(|e| ArchiveError::from(e))?;
        }
        Ok(())
    }

    /// send all finalized headers back to main thread
    pub async fn subscribe_finalized_heads(
        &self,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let mut stream = self.client.subscribe_finalized_blocks().await?;

        while let head = stream.next().await {
            sender
                .unbounded_send(Data::FinalizedHead(Header::new(head)))
                .map_err(|e| ArchiveError::from(e))?;
        }
        Ok(())
    }

    /// Fetch a block by hash from Substrate RPC
    pub async fn block(
        &self,
        hash: Option<T::Hash>,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        if let Some(block) = self.client.block(hash).await? {
            sender
                .unbounded_send(Data::Block(Block::new(block)))
                .map_err(Into::into)
        } else {
            log::warn!("No Block Exists!");
            Ok(())
        }
    }
}

/// Methods that return fetched value directly
impl<T> Rpc<T>
where
    T: Substrate + Send + Sync,
{
    pub(crate) async fn new(url: &str) -> Result<Self, ArchiveError> {
        
        let client = subxt::ClientBuilder::<T>::new().set_url(url).build().await?;

        Ok(Self {
            client,
        })
    }

    /// get the latest block
    pub(crate) async fn latest_block(&self) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        self.client.block::<T::Hash>(None).await.map_err(Into::into)
    }

    /// get just the latest header
    pub(crate) async fn latest_head(&self) -> Result<Option<T::Header>, ArchiveError> {
        self.client.header::<T::Hash>(None).await.map_err(Into::into)
    }

    pub(crate) async fn version(&self, hash: Option<&T::Hash>) -> Result<RuntimeVersion, ArchiveError> {
        self.client.runtime_version(hash).map_err(Into::into).await
    }
   
    pub async fn block_from_number(
        &self,
        number: NumberOrHex<T::BlockNumber>,
    ) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        let hash = self.client.block_hash(Some(number)).await?;
        self.client.block(hash).await.map_err(Into::into)
    }

    pub async fn batch_block_from_number(
        &self,
        numbers: Vec<NumberOrHex<T::BlockNumber>>,
    ) -> Result<Vec<SubstrateBlock<T>>, ArchiveError> {

        let mut blocks = Vec::new();
        for num in numbers.into_iter() {
            let block = self.block_from_number(num).await?;
            blocks.push(block);
        }

        Ok(blocks
            .into_iter()
            .filter_map(|b| b)
            .collect::<Vec<SubstrateBlock<T>>>())
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
