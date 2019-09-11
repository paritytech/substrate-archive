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

use futures::{Future, Stream, sync::mpsc, future};
use tokio::runtime::Runtime;
use tokio::util::StreamExt;
use failure::{Error as FailError, ResultExt};
use substrate_subxt::{Client, ClientBuilder, srml::system::System};

use crate::types::{Data, Payload};
use crate::error::{Error as ArchiveError};

// temporary util function to get a Substrate Client and Runtime
fn client<T: System + 'static>() -> (Runtime, Client<T>) {
    let mut rt = Runtime::new().unwrap();
    let client_future = ClientBuilder::<T>::new().build();
    let client = rt.block_on(client_future).unwrap();
    (rt, client)
}

pub fn run<T: System + std::fmt::Debug + 'static>() {
    let  (mut rt, client) = client::<T>();
    let (sender, receiver) = mpsc::unbounded();
    // rt.spawn(subscribe_new_heads(client.clone(), sender.clone()).map_err(|e| println!("{:?}", e)));
    // rt.spawn(subscribe_finalized_blocks(client.clone(), sender.clone()).map_err(|e| println!("{:?}", e)));
    rt.spawn(subscribe_events(client.clone(), sender.clone()).map_err(|e| println!("{:?}", e)));
    tokio::run(receiver.enumerate().for_each(|(i, data)| {
        println!("item: {}, {:?}", i, data);
        future::ok(())
    }));
}

fn subscribe_new_heads<T: System + 'static>(client: Client<T>, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
{
    client.subscribe_blocks()
        .map_err(|e| ArchiveError::from(e))
        .and_then(|stream| {
            stream.map_err(|e| e.into()).for_each(move |head| {
                sender.unbounded_send(Data {
                    payload: Payload::Header(head)
                }).map_err(|e| ArchiveError::from(e))
            })
        })
}

fn subscribe_finalized_blocks<T: System + 'static>(client: Client<T>, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
{
    client.subscribe_finalized_blocks()
        .map_err(|e| ArchiveError::from(e))
        .and_then(|stream| {
            stream.map_err(|e| e.into()).for_each(move |head| {
                sender.unbounded_send(Data {
                    payload: Payload::FinalizedHead(head)
                }).map_err(|e| ArchiveError::from(e))
            })
        })
}

fn subscribe_events<T: System + 'static>(client: Client<T>, sender: mpsc::UnboundedSender<Data<T>>) -> impl Future<Item = (), Error = ArchiveError>
{
    client.subscribe_events()
        .map_err(|e| ArchiveError::from(e))
        .and_then(|stream| {
            stream.map_err(|e| e.into()).for_each(move |storage_change| {
                sender.unbounded_send(Data {
                    payload: Payload::Event(storage_change),
                }).map_err(|e| ArchiveError::from(e))
            })
        })
}

/*
fn query_block<T: System + 'static>(client: Client<T>, sender: mpsc::UnboundedSender<Data<T>>, head: T::Header) -> impl Future<Item = (), Error = substrate_subxt::Error>
{
    unimplemented!()
}
*/


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
