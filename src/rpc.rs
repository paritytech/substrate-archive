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

mod substrate_rpc;
use self::substrate_rpc::SubstrateRpc;

use futures::{
    channel::mpsc::UnboundedSender,
    future::{self, FutureExt, TryFutureExt},
    stream::StreamExt,
};
use log::{debug, error, trace, warn};
use runtime_primitives::traits::Header as HeaderTrait;
use substrate_primitives::{storage::StorageKey, twox_128};
// use substrate_rpc_api::system::Properties;
use substrate_rpc_primitives::{list::ListOrValue, number::NumberOrHex};

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    error::Error as ArchiveError,
    metadata::Metadata,
    types::{BatchBlock, Block, Data, Header, Storage, SubstrateBlock, System},
};

/// Communicate with Substrate node via RPC
pub struct Rpc<T: System> {
    _marker: PhantomData<T>,
    url: url::Url,
    keys: Vec<StorageKey>,
    metadata: Metadata,
    // properties: Properties,
}

impl<T> Rpc<T>
where
    T: System,
{
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn keys(&self) -> &Vec<StorageKey> {
        &self.keys
    }
    /*
        pub fn properties(&self) -> &Properties {
            &self.properties
        }
    */
}

impl<T> Rpc<T>
where
    T: System,
{
    /// subscribes to new heads but sends blocks and timestamps instead of headers
    pub async fn subscribe_blocks(
        self: Arc<Self>,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let client = Arc::new(self.client().await?);
        let mut stream = client.subscribe_finalized_heads().await?;
        while let Some(head) = stream.next().await {
            log::info!("Got Head: {:?}", head);
            self.clone()
                .block(Some(head?.hash()), sender.clone())
                .await?;
        }
        Ok(())
    }
}

impl<T> Rpc<T>
where
    T: System,
{
    pub(crate) async fn new(url: url::Url) -> Result<Self, ArchiveError> {
        let client: SubstrateRpc<T> = SubstrateRpc::connect(&url).await?;

        let (keys, metadata) = futures::join! {
            client.storage_keys(StorageKey(Vec::new()), None),
            client.metadata(None),
        };

        Ok(Self {
            url,
            keys: keys?,
            metadata: metadata?,
            _marker: PhantomData,
        })
    }

    /// get the latest block
    pub(crate) async fn latest_block(&self) -> Result<Option<SubstrateBlock<T>>, ArchiveError> {
        let client = self.client().await?;
        let block = client.block(ListOrValue::Value(None)).await?;
        match block {
            ListOrValue::Value(v) => Ok(v),
            ListOrValue::List(_) => {
                return Err(ArchiveError::UnexpectedType(
                    "Expected Value, Got List".to_string(),
                ))
            }
        }
    }

    /// get just the latest header
    pub(crate) async fn latest_head(&self) -> Result<Option<T::Header>, ArchiveError> {
        let client = self.client().await?;
        client.header(None).await
    }

    pub async fn client(&self) -> Result<SubstrateRpc<T>, ArchiveError> {
        SubstrateRpc::connect(&self.url).await
    }

    /// send all new headers back to main thread
    pub async fn subscribe_new_heads(
        &self,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let client = self.client().await?;
        let mut stream = client.subscribe_new_heads().await?;

        while let Some(head) = stream.next().await {
            sender
                .unbounded_send(Data::Header(Header::new(head?)))
                .map_err(|e| ArchiveError::from(e))?;
        }
        Ok(())
    }

    /// send all finalized headers back to main thread
    pub async fn subscribe_finalized_heads(
        &self,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let client = self.client().await?;
        let mut stream = client.subscribe_finalized_heads().await?;

        while let Some(head) = stream.next().await {
            sender
                .unbounded_send(Data::FinalizedHead(Header::new(head?)))
                .map_err(|e| ArchiveError::from(e))?;
        }
        Ok(())
    }

    /*
    /// send all substrate events back to us
    pub fn subscribe_events(&self, sender: UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain.subscribe_events()
            .map_err(|e| ArchiveError::from(e))
            .and_then(|stream| {
                stream.map_err(|e| e.into()).for_each(move |storage_change| {
                    sender
                        .unbounded_send(Data::Event(storage_change))
                        .map_err(|e| ArchiveError::from(e))
                })
            })
    }
     */

    /*
        pub fn refresh_metadata(&mut self, hash: Option<T::Hash>) -> impl Future<Item = (), Error = ArchiveError> {
            SubstrateRpc::connect(&self.url)
                .and_then(move |client: SubstrateRpc<T>| {
                    client.metadata(hash)
                          .and_then(|m| {
                              self.metadata = m;
                              future::ok(())
                          })
                })
        }
    */

    pub async fn refresh_metadata(&mut self, hash: Option<T::Hash>) -> Result<(), ArchiveError> {
        let client = self.client().await?;
        let meta = client.metadata(hash).await?;
        self.metadata = meta;
        Ok(())
    }

    // TODO: make "Key" and "from" vectors
    // TODO: Merge 'from' and 'key' via a macro_derive on StorageKeyType, to auto-generate storage keys
    /// Get a storage item
    /// must provide the key, hash of the block to get storage from, as well as the key type
    pub async fn storage(
        &self,
        sender: UnboundedSender<Data<T>>,
        key: StorageKey,
        hash: T::Hash,
    ) -> Result<(), ArchiveError> {
        let client = self.client().await?;
        let storage = client.storage(key, hash).await?;
        debug!("STORAGE: {:?}", storage);
        if let Some(s) = storage {
            trace!("Sending timestamp for {}", hash);
            sender.unbounded_send(Data::Storage(Storage::new(s, hash)))?;
            Ok(())
        } else {
            warn!("Storage Item does not exist!");
            Ok(())
        }
    }

    pub async fn batch_storage(
        &self,
        keys: Vec<StorageKey>,
        hashes: Vec<T::Hash>,
    ) -> Result<Vec<Storage<T>>, ArchiveError> {
        assert!(hashes.len() == keys.len()); // TODO remove assertion, make into ensure!
                                             // TODO: too many clones
        let client = self.client().await?;
        let mut futures = Vec::new();
        for (idx, hash) in hashes.into_iter().enumerate() {
            let key = keys[idx].clone();
            futures.push(client.storage(key.clone(), hash).map(move |data| {
                if let Some(d) = data? {
                    Ok(Storage::new(d, hash))
                } else {
                    let err = format!("Storage item {:?} does not exist!", key);
                    Err(ArchiveError::DataNotFound(err))
                }
            }));
        }

        let data = future::join_all(futures).await;
        Ok(data
            .into_iter()
            .filter_map(|d| {
                if d.is_err() {
                    error!("{:?}", d);
                }
                d.ok()
            })
            .collect::<Vec<Storage<T>>>())
    }

    /// Fetch a block by hash from Substrate RPC
    pub async fn block(
        &self,
        hash: Option<T::Hash>,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let client = self.client().await?;
        let block = client.block(ListOrValue::Value(hash)).await?;
        Self::send_block(block, sender)
    }

    pub async fn block_from_number(
        &self,
        number: NumberOrHex<T::BlockNumber>,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        let client = self.client().await?;

        let num = Some(ListOrValue::Value(number));
        let block = client.block(client.hash(num).await?).await?;
        Self::send_block(block, sender)
    }

    pub async fn batch_block_from_number(
        &self,
        numbers: Vec<NumberOrHex<T::BlockNumber>>,
    ) -> Result<Vec<SubstrateBlock<T>>, ArchiveError> {
        let client = Arc::new(self.client().await?);

        let numbers = Some(ListOrValue::List(numbers));
        let blocks = client.block_from_number(numbers).await?;
        let blocks = match blocks {
            ListOrValue::Value(_) => {
                return Err(ArchiveError::UnexpectedType(
                    "Expected List, got Value".to_string(),
                ))
            }
            ListOrValue::List(v) => v,
        };

        Ok(blocks
            .into_iter()
            .filter_map(|b| b)
            .collect::<Vec<SubstrateBlock<T>>>())
    }

    fn send_block(
        block: ListOrValue<Option<SubstrateBlock<T>>>,
        sender: UnboundedSender<Data<T>>,
    ) -> Result<(), ArchiveError> {
        match block {
            ListOrValue::Value(v) => {
                if let Some(b) = v {
                    sender
                        .unbounded_send(Data::Block(Block::new(b)))
                        .map_err(Into::into)
                } else {
                    warn!("No Block Exists!");
                    Ok(())
                }
            }
            ListOrValue::List(v) => {
                // throws out any none's
                let blocks = v
                    .into_iter()
                    .filter_map(|b| b)
                    .collect::<Vec<SubstrateBlock<T>>>();
                sender
                    .unbounded_send(Data::BatchBlock(BatchBlock::new(blocks)))
                    .map_err(Into::into)
            }
        }
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
