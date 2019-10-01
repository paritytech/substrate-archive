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
use futures::{Future, Stream, sync::mpsc, future};
use tokio::runtime::Runtime;
use jsonrpc_core_client::{RpcChannel, transports::ws};
use runtime_primitives::traits::Header;
use substrate_rpc_api::{
    author::AuthorClient,
    chain::{
        ChainClient,
    },
    state::StateClient,
};

use crate::types::{Data, System, Block};
use crate::error::{Error as ArchiveError};
use crate::database::Database;


fn handle_data<T>(receiver: mpsc::UnboundedReceiver<Data<T>>,
                  rpc: Rpc<T>,
                  sender: mpsc::UnboundedSender<Data<T>>,
                  db: Database
) -> impl Future<Item = (), Error = ()> + 'static
where T: System + std::fmt::Debug + 'static
{
    // spawn a getter for blocks if not there
    // else insert the value into the database
    receiver.for_each(move |data| {
        match &data {
            Data::Header(header) /* Data::FinalizedHead(header) */ => {
                tokio::spawn(
                    rpc.block(header.hash(), sender.clone())
                       .map_err(|e| println!("{:?}", e))
                );
            },
            _ => {}
        };
        db.insert(&data);
        future::ok(())
    })
}

pub fn run<T: System + std::fmt::Debug + 'static>() -> Result<(), ArchiveError>{
    let (sender, receiver) = mpsc::unbounded();
    let mut rt = Runtime::new()?;
    let rpc = Rpc::<T>::new(&mut rt, &url::Url::parse("ws://127.0.0.1:9944")?)?;
    let db = Database::new();
    rt.spawn(rpc.subscribe_new_heads(sender.clone()).map_err(|e| println!("{:?}", e)));
    rt.spawn(rpc.subscribe_finalized_blocks(sender.clone()).map_err(|e| println!("{:?}", e)));
    // rt.spawn(rpc.subscribe_events(sender.clone()).map_err(|e| println!("{:?}", e)));
    tokio::run(handle_data(receiver, rpc, sender, db));
    Ok(())
}



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
    #[allow(dead_code)] // TODO remove
    state: StateClient<T::Hash>, // TODO get types right
    chain: ChainClient<T::BlockNumber, T::Hash, <T as System>::Header, Block<T>>,
    #[allow(dead_code)] // TODO remove
    author: AuthorClient<T::Hash, T::Hash>, // TODO get types right
}

impl<T> Rpc<T> where T: System + 'static {

    /// instantiate new client
    pub fn new(rt: &mut Runtime, url: &url::Url) -> Result<Self, ArchiveError> {
        rt.block_on(ws::connect(url).map_err(Into::into))
    }

    /// send all new headers back to main thread
    pub fn subscribe_new_heads(&self, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain.subscribe_new_heads()
            .map_err(|e| ArchiveError::from(e))
            .and_then(|stream| {
                stream.map_err(|e| e.into()).for_each(move |head| {
                    sender
                        .unbounded_send(Data::Header(head))
                        .map_err(|e| ArchiveError::from(e))
                })
            })
    }

    /// send all finalized headers back to main thread
    pub fn subscribe_finalized_blocks(&self, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain.subscribe_finalized_heads()
            .map_err(|e| ArchiveError::from(e))
            .and_then(|stream| {
                stream.map_err(|e| e.into()).for_each(move |head| {
                    sender
                        .unbounded_send(Data::FinalizedHead(head))
                        .map_err(|e| ArchiveError::from(e))
                })
            })
    }
/*
    /// send all substrate events back to main thread
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

    fn block(&self, hash: T::Hash, sender: mpsc::UnboundedSender<Data<T>>)
             -> impl Future<Item = (), Error = ArchiveError>
    {
        self.chain
            .block(Some(hash))
            .map_err(Into::into)
            .and_then(move |block| {
                if let Some(b) = block {
                    sender
                        .unbounded_send(Data::Block(b))
                        .map_err(|e| ArchiveError::from(e))
                } else {
                    info!("No block exists! (somehow)");
                    Ok(()) // TODO: error Out
                }
            })
    }

    fn unsubscribe_finalized_heads() {
        unimplemented!();
    }

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
