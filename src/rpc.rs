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

use log::*;
use futures::{Future, Stream, sync::mpsc};
use tokio::runtime::Runtime;
use jsonrpc_core_client::{RpcChannel, transports::ws};
use substrate_primitives::{
    storage::StorageKey
};

use substrate_rpc_api::{
    author::AuthorClient,
    chain::{
        ChainClient,
    },
    state::StateClient,
};

use crate::types::{Data, System, SubstrateBlock, storage::StorageKeyType, Block, Header, Storage};
use crate::error::{Error as ArchiveError};


impl<T: System> From<RpcChannel> for Rpc<T> {
    fn from(channel: RpcChannel) -> Self {
        Self {
            state: channel.clone().into(),
            chain: channel.clone().into(),
            author: channel.into(),
        }
    }
}

/// Communicate with Substrate node via RPC
pub struct Rpc<T: System> {
    state: StateClient<T::Hash>,
    chain: ChainClient<T::BlockNumber, T::Hash, <T as System>::Header, SubstrateBlock<T>>,
    #[allow(dead_code)] // TODO remove
    author: AuthorClient<T::Hash, T::Hash>, // TODO get types right
}

impl<T> Rpc<T> where T: System + 'static {

    /// instantiate new client
    pub fn new(rt: &mut Runtime, url: &url::Url) -> Result<Self, ArchiveError> {
        rt.block_on(ws::connect(url).map_err(Into::into))
    }

    /// send all new headers back to main thread
    pub(crate) fn subscribe_new_heads(&self, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain.subscribe_new_heads()
            .map_err(|e| ArchiveError::from(e))
            .and_then(|stream| {
                stream.map_err(|e| e.into()).for_each(move |head| {
                    sender
                        .unbounded_send(Data::Header(Header::new(head)))
                        .map_err(|e| ArchiveError::from(e))
                })
            })
    }

    /// send all finalized headers back to main thread
    pub(crate) fn subscribe_finalized_blocks(&self, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain.subscribe_finalized_heads()
            .map_err(|e| ArchiveError::from(e))
            .and_then(|stream| {
                stream.map_err(|e| e.into()).for_each(move |head| {
                    sender
                        .unbounded_send(Data::FinalizedHead(Header::new(head)))
                        .map_err(|e| ArchiveError::from(e))
                })
            })
    }
    /*
    /// send all substrate events back to us
    pub fn subscribe_events(&self, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
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
    pub(crate) fn storage(&self, sender: mpsc::UnboundedSender<Data<T>>, key: StorageKey, hash: T::Hash, from: StorageKeyType )
               -> impl Future<Item = (), Error = ArchiveError>
    {
        self.state
            .storage(key, Some(hash))
            .map_err(Into::into)
            .and_then(move |data| {
                if let Some(d) = data {
                    sender
                        .unbounded_send(Data::Storage(Storage::new(d, from, hash)))
                        .map_err(|e| ArchiveError::from(e))
                } else {
                    warn!("Storage Item does not exist!");
                    Ok(())
                }
            })
    }
/*
    /// Get all storage keys
    pub(crate) fn storage_keys(&self, sender: mpsc::UnboundedSender<Data<T>>)
                    -> impl Future<Item = (), Error = ArchiveError>
    {
        self.state
            .storage_keys(StorageKey(Vec::new()), None)
            .map_err(Into::into)
            .and_then(move |keys| {
                for key in keys {
                    println!("{:?}", key);
                }
                future::ok(())
            })
    }
     */

    /// Fetch a block by hash from Substrate RPC
    pub(crate) fn block(&self, hash: T::Hash, sender: mpsc::UnboundedSender<Data<T>>)
             -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain
            .block(Some(hash))
            .map_err(Into::into)
            .and_then(move |block| {
                if let Some(b) = block {
                    sender
                        .unbounded_send(Data::Block(Block::new(b)))
                        .map_err(|e| ArchiveError::from(e))
                } else {
                    info!("No block exists! (somehow)");
                    Ok(()) // TODO: error Out
                }
            })
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

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
