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
use futures::{Future, Stream, sync::mpsc::{self, UnboundedReceiver, UnboundedSender}, future};
use tokio::runtime::Runtime;
use runtime_primitives::traits::Header;
use substrate_primitives::{
    storage::StorageKey,
    twox_128
};

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    database::Database,
    rpc::Rpc,
    error::Error as ArchiveError,
    types::{System, Data, storage::{StorageKeyType, TimestampOp}}
};

// TODO: the " 'static" constraint will be possible to remove Nov 7, with the hopeful and long-anticipated release of async-await
pub struct Archive<T: System + std::fmt::Debug + 'static> {
    marker: PhantomData<T>
}

impl<T> Archive<T> where T: System + std::fmt::Debug + 'static {

    pub fn run() -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        let mut rt = Runtime::new()?;
        let rpc = Rpc::<T>::new(&mut rt, &url::Url::parse("ws://127.0.0.1:9944")?)?;
        let db = Database::new()?;
        rt.spawn(rpc.subscribe_new_heads(sender.clone()).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.subscribe_finalized_blocks(sender.clone()).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.storage_keys(sender).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.subscribe_events(sender.clone()).map_err(|e| println!("{:?}", e)));
        let rpc = Arc::new(rpc);
        tokio::run(Self::handle_data(receiver, db, rpc.clone(), sender));
        Ok(())
    }

    /// Verify that all blocks are in the database
    fn verify(db: Database, rpc: Rpc<T>) -> () /* impl Future<Item = (), Error = ()> */ {
        unimplemented!();
    }

    fn handle_data(receiver: UnboundedReceiver<Data<T>>,
                    db: Database,
                    rpc: Arc<Rpc<T>>,
                    sender: UnboundedSender<Data<T>>,
    ) -> impl Future<Item = (), Error = ()> + 'static
    {
        // task for getting blocks
        // if we need data that depends on other data that needs to be received first (EX block needs hash from the header)
        receiver.for_each(move |data| {
            match &data {
                Data::Header(header) => {
                    tokio::spawn(
                        rpc.block(header.inner().hash(), sender.clone())
                        .map_err(|e| warn!("{:?}", e))
                    );
                },
                Data::Block(block) => {
                    let header = block.inner().block.header.clone();
                    let timestamp_key = b"Timestamp Now";
                    let storage_key = twox_128(timestamp_key);
                    let (sender, rpc) = (sender.clone(), rpc.clone());

                    tokio::spawn(
                        db.insert(&data)
                          .map_err(|e| warn!("{:?}", e))
                          .and_then(move |res| { // TODO do something with res
                              // send off storage (timestamps, etc) for
                              // this block hash to be inserted into the db
                              rpc.storage(
                                  sender,
                                  StorageKey(storage_key.to_vec()),
                                  header.hash(),
                                  StorageKeyType::Timestamp(TimestampOp::Now)
                              )
                                 .map_err(|e| warn!("{:?}", e))
                          })
                    );
                },
                _ => {
                    tokio::spawn(db.insert(&data).map_err(|e| warn!("{:?}", e)));
                }
            };
            future::ok(())
        })
    }
}
