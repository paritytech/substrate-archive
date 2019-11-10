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

use log::{debug, warn, error, trace};
use futures::{
    future::{FutureExt, TryFutureExt, join_all},
    stream::StreamExt,
    channel::mpsc::UnboundedSender
};
use runtime_primitives::traits::Header as HeaderTrait;
use substrate_primitives::{storage::StorageKey, twox_128};
use substrate_rpc_primitives::number::NumberOrHex;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    types::{
        storage::{StorageKeyType, TimestampOp},
        Data, System, SubstrateBlock,
        Block, BatchBlock, BatchStorage,
        Header, Storage,
    },
    metadata::Metadata,
    error::Error as ArchiveError,
};

/// Communicate with Substrate node via RPC
pub struct Rpc<T: System> {
    _marker: PhantomData<T>,
    url: url::Url
}


impl<T> Rpc<T> where T: System {
    /// subscribes to new heads but sends blocks and timestamps instead of headers
    pub async fn subscribe_blocks(self: Arc<Self>, sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        let client = Arc::new(self.client().await?);
        let stream = client.subscribe_finalized_heads().await?;
        stream.for_each(move |head: Result<T::Header, ArchiveError>| {
            async { // task will be executed in 'isolation' so we handle the error directly
                let sender0 = sender.clone();
                match self.clone().block(head.unwrap().hash(), sender0).await {
                    Err(e) => error!("{:?}", e),
                    Ok(_) => ()
                };
            }
        });
        Ok(())
    }

    pub async fn block_and_timestamp(self: Arc<Self>, hash: T::Hash, sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        trace!("Gathering block + timestamp for {}", hash);
        let rpc = self.clone();
        let client = self.client().await?;
        let client = Arc::new(client);

        let (sender0, sender1) = (sender.clone(), sender.clone());

        let block = client.block(Some(hash)).await?;
        Self::send_block(block, sender0.clone())?;

        let timestamp_key = b"Timestamp Now";
        let storage_key = twox_128(timestamp_key);
        let key = StorageKey(storage_key.to_vec());
        let key_type = StorageKeyType::Timestamp(TimestampOp::Now);
        rpc.storage(sender1, key, hash, key_type).await
    }
}

impl<T> Rpc<T> where T: System {

    pub fn new(url: url::Url) -> Self {
        Self {
            url,
            _marker: PhantomData
        }
    }

    pub async fn client(&self) -> Result<SubstrateRpc<T>, ArchiveError> {
        SubstrateRpc::connect(&self.url).await
    }

    /// send all new headers back to main thread
    pub async fn subscribe_new_heads(&self, sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        let client = self.client().await?;
        let stream = client.subscribe_new_heads().await?;

        for head in stream.next().await {
            sender.unbounded_send(Data::Header(Header::new(head?)))
                  .map_err(|e| ArchiveError::from(e))?;
        }
        Ok(())
    }

    /// send all finalized headers back to main thread
    pub async fn subscribe_finalized_heads(&self, sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        let client = self.client().await?;
        let stream = client.subscribe_finalized_heads().await?;

        for head in stream.next().await {
            sender.unbounded_send(Data::FinalizedHead(Header::new(head?)))
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

    pub async fn metadata(&self) -> Result<Metadata, ArchiveError> {
        let client = self.client().await?;
        client.metadata().await
    }

    // TODO: make "Key" and "from" vectors
    // TODO: Merge 'from' and 'key' via a macro_derive on StorageKeyType, to auto-generate storage keys
    /// Get a storage item
    /// must provide the key, hash of the block to get storage from, as well as the key type
    pub async fn storage(&self,
                          sender: UnboundedSender<Data<T>>,
                          key: StorageKey,
                          hash: T::Hash,
                          key_type: StorageKeyType
    ) -> Result<(), ArchiveError> {

        let client = self.client().await?;
        let storage = client.storage(key, hash).await?;
        debug!("STORAGE: {:?}", storage);
        if let Some(s) = storage {
            trace!("Sending timestamp for {}", hash);
            sender
                .unbounded_send(Data::Storage(Storage::new(s, key_type, hash)))
                .map_err(Into::into)
        } else {
            warn!("Storage Item does not exist!");
            Ok(())
        }
    }

    pub async fn batch_storage(&self,
                         sender: UnboundedSender<Data<T>>,
                         keys: Vec<StorageKey>,
                         hashes: Vec<T::Hash>,
                         key_types: Vec<StorageKeyType>
    ) -> Result<(), ArchiveError>
    {
        assert!(hashes.len() == keys.len() && keys.len() == key_types.len()); // TODO remove assertion
        // TODO: too many clones
        let client = self.client().await?;
        let mut futures = Vec::new();
        for (idx, hash) in hashes.into_iter().enumerate() {
            let key = keys[idx].clone();
            let key_type = key_types[idx].clone();
            futures.push(
                client.storage(key.clone(), hash)
                      .map(move |data| {
                          if let Some(d) = data? {
                              Ok(Storage::new(d, key_type, hash))
                          } else {
                              let err = format!("Storage item {:?} does not exist!", key);
                              Err(ArchiveError::DataNotFound(err))
                          }
                      })
            );
        }

        let data = join_all(futures).await;
        let data = data.into_iter()
            .filter_map(|d| {
                if d.is_err() { error!("{:?}", d); }
                d.ok()
            }).collect::<Vec<Storage<T>>>();
        sender.unbounded_send(Data::BatchStorage(BatchStorage::new(data)))
              .map_err(Into::into)
    }

    /// Fetch a block by hash from Substrate RPC
    pub async fn block(&self, hash: T::Hash, sender: UnboundedSender<Data<T>>) -> Result<(), ArchiveError> {
        let client = self.client().await?;
        let block = client.block(Some(hash)).await?;
        Self::send_block(block, sender)
    }

    pub async fn block_from_number(&self,
                       number: NumberOrHex<T::BlockNumber>,
                       sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        let client = self.client().await?;
        let block = client.block(client.hash(number).await?).await?;
        Self::send_block(block, sender)
    }

    pub async fn batch_block_from_number(&self,
                                          numbers: Vec<NumberOrHex<T::BlockNumber>>,
                                          sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        let client = Arc::new(self.client().await?);
        let mut futures = Vec::new();
        for number in numbers {
            let client = client.clone();
            futures.push(
                client.hash(number)
                      .and_then(move |hash| {
                          client.block(hash)
                      })
            );
        }

        let blocks =
            join_all(futures)
            .await
            .into_iter()
            .filter_map(|b| b.transpose())
            .filter_map(|b| {
                if b.is_err() { error!("{:?}", b); }
                b.ok()
            })
            .collect::<Vec<SubstrateBlock<T>>>();
        sender.unbounded_send(Data::BatchBlock(BatchBlock::new(blocks)))
              .map_err(Into::into)
    }

    fn send_block(block: Option<SubstrateBlock<T>>, sender: UnboundedSender<Data<T>>
    ) -> Result<(), ArchiveError>
    {
        if let Some(b) = block {
            sender.unbounded_send(Data::Block(Block::new(b)))
                .map_err(Into::into)
        } else {
            warn!("No Block Exists!");
            Ok(())
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
    fn can_query_blocks() {

    }
}
