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
use futures::{Future, Stream, sync::mpsc::UnboundedSender, future::join_all};
use runtime_primitives::traits::Header as HeaderTrait;
use substrate_primitives::{storage::{StorageKey}, twox_128};
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
    error::{Error as ArchiveError},
};

/// Communicate with Substrate node via RPC
pub struct Rpc<T: System> {
    _marker: PhantomData<T>,
    url: url::Url
}

impl<T> Rpc<T> where T: System {
    /// subscribes to new heads but sends blocks and timestamps instead of headers
    pub fn subscribe_blocks(self: Arc<Self>, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {

        let rpc = self.clone();
        SubstrateRpc::connect(&self.url)
            .and_then(|client: SubstrateRpc<T>| {
                let client = Arc::new(client);
                client.subscribe_finalized_heads()
                       .and_then(|stream| {
                           stream.for_each(move |head| {
                               let sender0 = sender.clone();
                               rpc.clone().block(head.hash(), sender0)
                           })
                       })
            })
    }

    pub fn block_and_timestamp(self: Arc<Self>, hash: T::Hash, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        trace!("Gathering block + timestamp for {}", hash);
        let rpc = self.clone();
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                let sender1 = sender.clone();
                let sender2 = sender.clone();
                client.
                    block(Some(hash))
                    .and_then(move |block| {
                        Self::send_block(block, sender1)
                    }).and_then(move |_| {
                        let timestamp_key = b"Timestamp Now";
                        let storage_key = twox_128(timestamp_key);
                        let key = StorageKey(storage_key.to_vec());
                        let key_type = StorageKeyType::Timestamp(TimestampOp::Now);
                        rpc.storage(sender2, key, hash, key_type)
                    })
            })
    }
}


impl<T> Rpc<T> where T: System {

    pub fn new(url: url::Url) -> Self {
        Self {
            url,
            _marker: PhantomData
        }
    }

    /// send all new headers back to main thread
    pub fn subscribe_new_heads(&self, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(|client: SubstrateRpc<T>| {
                client.subscribe_new_heads()
                      .and_then(|stream| {
                          stream
                              .for_each(move |head| {

                                  sender.unbounded_send(Data::Header(Header::new(head)))
                                        .map_err(|e| ArchiveError::from(e))
                          })
                      })
            })
    }

    /// send all finalized headers back to main thread
    pub fn subscribe_finalized_heads(&self, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(|client: SubstrateRpc<T>| {
                client
                    .subscribe_finalized_heads()
                    .and_then(|stream| {
                        stream.for_each(move |head| {
                            sender.unbounded_send(Data::FinalizedHead(Header::new(head)))
                                  .map_err(Into::into)
                        })
                    })
            })
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

    pub fn metadata(&self) -> impl Future<Item = Metadata, Error = ArchiveError> {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client.metadata()
            })
    }

    // TODO: make "Key" and "from" vectors
    // TODO: Merge 'from' and 'key' via a macro_derive on StorageKeyType, to auto-generate storage keys
    /// Get a storage item
    /// must provide the key, hash of the block to get storage from, as well as the key type
    pub fn storage(&self,
                          sender: UnboundedSender<Data<T>>,
                          key: StorageKey,
                          hash: T::Hash,
                          key_type: StorageKeyType
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client
                    .storage(key, hash)
                    .and_then(move |data| {
                        debug!("STORAGE: {:?}", data);
                        if let Some(d) = data {
                            trace!("Sending timestamp for {}", hash);
                            sender
                                .unbounded_send(Data::Storage(Storage::new(d, key_type, hash)))
                                .map_err(Into::into)
                        } else {
                            warn!("Storage Item does not exist!");
                            Ok(())
                        }
                    })
            })
    }

    pub fn batch_storage(&self,
                         sender: UnboundedSender<Data<T>>,
                         keys: Vec<StorageKey>,
                         hashes: Vec<T::Hash>,
                         key_types: Vec<StorageKeyType>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        assert!(hashes.len() == keys.len() && keys.len() == key_types.len()); // TODO remove assertion
        // TODO: too many clones
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                let mut futures = Vec::new();
                for (idx, hash) in hashes.into_iter().enumerate() {
                    let key = keys[idx].clone();
                    let key_type = key_types[idx].clone();
                    futures.push(
                        client.storage(key.clone(), hash)
                              .map(move |data| {
                                  if let Some(d) = data {
                                      Ok(Storage::new(d, key_type, hash))
                                  } else {
                                      let err = format!("Storage item {:?} does not exist!", key);
                                      Err(ArchiveError::DataNotFound(err))
                                  }
                              })
                    );
                }
                join_all(futures)
                    .and_then(move |data| {
                        let data = data.into_iter().filter_map(|d| {
                            if d.is_err() { error!("{:?}", d); }
                            d.ok()
                        }).collect::<Vec<Storage<T>>>();
                        sender.unbounded_send(Data::BatchStorage(BatchStorage::new(data)))
                            .map_err(Into::into)
                    })
            })
    }

    /// Fetch a block by hash from Substrate RPC
    pub fn block(&self, hash: T::Hash, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client.
                    block(Some(hash))
                    .and_then(move |block| {
                        Self::send_block(block.clone(), sender)
                    })
            })
    }

    pub fn block_from_number(&self,
                       number: NumberOrHex<T::BlockNumber>,
                       sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client.hash(number)
                      .and_then(move |hash| {
                          client.block(hash)
                                .and_then(move |block| {
                                    Self::send_block(block, sender) // TODO
                                })
                      })
            })
    }

    pub fn batch_block_from_number(&self,
                                          numbers: Vec<NumberOrHex<T::BlockNumber>>,
                                          sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                let client = Arc::new(client);

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
                join_all(futures)
                    .and_then(move |blocks| {
                        let blocks = blocks.into_iter().filter_map(|b| b).collect::<Vec<SubstrateBlock<T>>>();
                        sender.unbounded_send(Data::BatchBlock(BatchBlock::new(blocks)))
                            .map_err(Into::into)
                    })
            })
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
