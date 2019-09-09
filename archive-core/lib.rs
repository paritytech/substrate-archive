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


mod rpc;
mod types;
mod error;
use futures::{sync::mpsc, stream::Stream, future::{self, Future}};
use tokio::util::StreamExt;
pub use substrate_subxt::srml::system::System;
use sr_primitives::traits::Block;

pub fn run<T: System + Block>() {
    let  (mut rt, client) = rpc::client::<T>();
    let (sender, receiver) = mpsc::unbounded();
    rt.spawn(rpc::subscribe_new_heads(client.clone(), sender.clone()).map_err(|e| println!("{:?}", e)));
    rt.spawn(rpc::subscribe_finalized_blocks(client.clone(), sender.clone()).map_err(|e| println!("{:?}", e)));
    tokio::run(receiver.enumerate().for_each(|(i, data)| {
        println!("item: {}, {:?}", i, data);
        future::ok(())
    }));
}
