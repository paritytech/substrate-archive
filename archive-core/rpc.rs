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

use futures::{Future, Stream, sync::mpsc};
use failure::{Error, ResultExt};
use tokio::runtime::Runtime;
use sr_primitives::traits::{Block};
use substrate_subxt::{Client, ClientBuilder, srml::system::System};

use crate::error::ErrorKind;
use crate::types::{Data, DataEntryType, Payload};


// temporary util function to get a Substrate Client and Runtime
pub fn client<T: System + Block>() -> (Runtime, Client<T>) {
    let mut rt = Runtime::new().unwrap();
    let client_future = ClientBuilder::<T>::new().build();
    let client = rt.block_on(client_future).unwrap();
    (rt, client)
}

/*
pub fn query_block(client: ChainClient<Hash, Hash, Header, SignedBlock<Block>>, block_num: u64)
    -> impl Future<Item = SignedBlock<Block>, Error = RpcError>
{
    client.
        block_hash(Some(NumberOrHex::Hex(U256::from(block_num))))
        .and_then(move |hash| {
            client.block(hash)
        })
        .map(|b| { b.ok_or(RpcError::Other(ErrorKind::ValueNotPresent.into())) })
        .and_then(|b| { b })
}
 */

pub fn subscribe_new_heads<T: System + Block>(client: Client<T>, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = substrate_subxt::Error>
{
    client.subscribe_blocks()
          .and_then(|stream| {
              stream.for_each(move |_head| {
                  sender.unbounded_send(Data {
                      info: DataEntryType::NewHead,
                      payload: Payload::None
                  }).map_err(|e| println!("{:?}", e)).unwrap();
                  futures::future::ok(())
              }).map_err(|e| substrate_subxt::Error::Rpc(e))
          })
}

pub fn subscribe_finalized_blocks<T: System + Block>(client: Client<T>, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = substrate_subxt::Error>
{
    client.subscribe_finalized_blocks()
        .and_then(|stream| {
            stream.for_each(move |_head| {
                sender.unbounded_send(Data {
                    info: DataEntryType::FinalizedBlock,
                    payload: Payload::None
                }).map_err(|e| println!("{:?}", e)).unwrap();
                futures::future::ok(())
            }).map_err(|e| substrate_subxt::Error::Rpc(e))
        })
}

fn unsubscribe_finalized_heads() {
    unimplemented!();
}

fn unsubscribe_new_heads() {
    unimplemented!();
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
