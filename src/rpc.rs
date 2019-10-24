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

use log::*;
use futures::{Future, Stream, sync::mpsc::UnboundedSender, future::{self, join_all}};
use tokio::runtime::Runtime;
use jsonrpc_core_client::{RpcChannel, transports::ws};
use runtime_primitives::traits::Header as HeaderTrait;
use substrate_primitives::storage::StorageKey;
use substrate_rpc_primitives::number::NumberOrHex;
use substrate_rpc_api::{
    author::AuthorClient,
    chain::{
        ChainClient,
    },
    state::StateClient,
};

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    types::{
        storage::StorageKeyType,
        Data, System, SubstrateBlock,
        Block, BatchBlock, Header, Storage,
    },
    error::{Error as ArchiveError},
};

/// Communicate with Substrate node via RPC
pub struct Rpc<T: System> {
    _marker: PhantomData<T>,
    url: url::Url
}

impl<T> Rpc<T> where T: System {

    pub(crate) fn new(url: url::Url) -> Self {
        Self {
            url,
            _marker: PhantomData
        }
    }

    /// send all new headers back to main thread
    pub(crate) fn subscribe_new_heads(&self, sender: UnboundedSender<Data<T>>
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

    /// subscribes to new heads but sends blocks instead of headers
    pub(crate) fn subscribe_blocks(&self, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(|client: SubstrateRpc<T>| {
                let client = Arc::new(client);
                let client0 = client.clone();
                let client1 = client.clone();
                client0.subscribe_finalized_heads()
                      .and_then(|stream| {
                          stream.for_each(move |head| {
                              let sender0 = sender.clone();
                              client1
                                  .block(head.hash())
                                  .and_then(move |block| {
                                      Self::send_block(block, sender0)
                                  })
                          })
                      })
            })
    }

    /// send all finalized headers back to main thread
    pub(crate) fn subscribe_finalized_heads(&self, sender: UnboundedSender<Data<T>>
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

    // TODO: make "Key" and "from" vectors
    // TODO: Merge 'from' and 'key' via a macro_derive on StorageKeyType, to auto-generate storage keys
    /// Get a storage item
    /// must provide the key, hash of the block to get storage from, as well as the key type
    pub(crate) fn storage(&self,
                          sender: UnboundedSender<Data<T>>,
                          key: StorageKey,
                          hash: T::Hash,
                          from: StorageKeyType
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client
                    .storage(key, hash)
                    .and_then(move |data| {
                        if let Some(d) = data {
                            sender
                                .unbounded_send(Data::Storage(Storage::new(d, from, hash)))
                                .map_err(Into::into)
                        } else {
                            warn!("Storage Item does not exist!");
                            Ok(())
                        }
                    })
            })
    }

    /// Fetch a block by hash from Substrate RPC
    pub(crate) fn block(&self, hash: T::Hash, sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client.
                    block(hash)
                    .and_then(move |block| {
                        Self::send_block(block, sender)
                    })
            })
    }

    pub(crate) fn block_from_number(&self,
                       number: NumberOrHex<T::BlockNumber>,
                       sender: UnboundedSender<Data<T>>
    ) -> impl Future<Item = (), Error = ArchiveError>
    {
        SubstrateRpc::connect(&self.url)
            .and_then(move |client: SubstrateRpc<T>| {
                client.hash(number)
                      .and_then(move |hash| {
                          client.block(hash.expect("Should always exist"))
                                .and_then(move |block| {
                                    Self::send_block(block, sender)
                                })
                      })
            })
    }

    pub(crate) fn batch_block_from_number(&self,
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
                    let sender = sender.clone();
                    futures.push(
                        client.hash(number)
                            .and_then(move |hash| {
                                client.block(hash.expect("should always exist"))
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
    fn unsubscribe_finalized_heads() {
        unimplemented!();
    }

    /// unsubscribe from new heads
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
