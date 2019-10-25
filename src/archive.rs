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

//! Spawning of all tasks happens in this module
//! Nowhere else is anything ever spawned

use log::*;
use futures::{
    Future, Stream,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    future::{self, loop_fn, Loop}
};
use tokio::runtime::Runtime;
use runtime_primitives::traits::Header;
use substrate_rpc_primitives::number::NumberOrHex;
use substrate_primitives::{
    U256,
    storage::StorageKey,
    twox_128
};

use std::{
    sync::Arc,
    thread,
    time
};

use crate::{
    database::Database,
    rpc::Rpc,
    error::Error as ArchiveError,
    types::{System, Data, storage::{StorageKeyType, TimestampOp}}
};

// TODO: the " 'static" constraint will be possible to remove Nov 7,
// with the hopeful and long-anticipated release of async-await
pub struct Archive<T: System> {
    rpc: Arc<Rpc<T>>,
    db: Arc<Database>,
    runtime: Runtime
}

impl<T> Archive<T> where T: System {

    pub fn new() -> Result<Self, ArchiveError> {
        let mut runtime = Runtime::new()?;
        let rpc = Rpc::<T>::new(url::Url::parse("ws://127.0.0.1:9944")?);
        let db = Database::new()?;
        let (rpc, db) = (Arc::new(rpc), Arc::new(db));
        let metadata = runtime.block_on(rpc.metadata())?;
        debug!("METADATA: {:?}", metadata);
        Ok( Self { rpc, db, runtime })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        crate::util::init_logger(log::LevelFilter::Error); // TODO move this into polkadot-archive
        self.runtime.spawn(self.rpc.subscribe_blocks(sender.clone()).map_err(|e| println!("{:?}", e)));
        // self.runtime.spawn(self.rpc.subscribe_finalized_heads(sender.clone()).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.storage_keys(sender).map_err(|e| println!("{:?}", e)));
        // rt.spawn(rpc.subscribe_events(sender.clone()).map_err(|e| println!("{:?}", e)));
        self.runtime.spawn(
            Self::sync(self.db.clone(), self.rpc.clone(), sender.clone())
                .map(|v| info!("updated {} mising blocks", v))
        );
        tokio::run(Self::handle_data(receiver, self.db.clone(), self.rpc.clone(), sender));
        Ok(())
    }
    // TODO return a float between 0 and 1 corresponding to percent of database that is up-to-date?
    /// Verification task that ensures all blocks are in the database
    fn sync(db: Arc<Database>,
              rpc: Arc<Rpc<T>>,
              sender: UnboundedSender<Data<T>>
    )
            -> impl Future<Item = Sync, Error = ()> + 'static
    {
        loop_fn(Sync::new(), move |v| {
            let (sender0, sender1) = (sender.clone(), sender.clone());
            v.sync(db.clone(), rpc.clone(), sender0.clone())
             .and_then(move |(sync, done)| {
                 info!("Updating {} missing blocks", sync);
                 sender1.unbounded_send(Data::SyncProgress(sync.blocks_missing))
                       .unwrap();
                 if done {
                     Ok(Loop::Break(sync))
                 } else {
                     Ok(Loop::Continue(sync))
                 }
             })
        })
    }

    fn handle_data(receiver: UnboundedReceiver<Data<T>>,
                    db: Arc<Database>,
                    rpc: Arc<Rpc<T>>,
                    sender: UnboundedSender<Data<T>>,
    ) -> impl Future<Item = (), Error = ()> + 'static
    {
        // task for getting blocks
        // if we need data that depends on other data that needs to be received first
        // (EX block needs hash from the header)
        receiver.for_each(move |data| {
            match &data {
                Data::Block(block) => {
                    // TODO: Transfer this into the rpc
                    // So that it does not starve RPC of connections
                    let header = block.inner().block.header.clone();
                    let timestamp_key = b"Timestamp Now";
                    let storage_key = twox_128(timestamp_key);
                    let (sender, rpc) = (sender.clone(), rpc.clone());

                    tokio::spawn(
                        db.insert(&data)
                          .map_err(|e| warn!("{:?}", e))
                          .and_then(move |_| {
                              // send off storage (timestamps, etc) for
                              // this block hash to be inserted into the db
                              rpc.storage(
                                   sender,
                                   StorageKey(storage_key.to_vec()),
                                   Some(header.hash()),
                                   StorageKeyType::Timestamp(TimestampOp::Now)
                              ).map_err(|e| warn!("{:?}", e))
                          })
                    );
                },
                Data::SyncProgress(missing_blocks) => {
                    println!("{} blocks missing", missing_blocks);
                },
                Data::BatchBlock(_blocks) => {
                    let timestamp_key = b"Timestamp Now";
                    let storage_key = twox_128(timestamp_key);
                    let (sender, rpc) = (sender.clone(), rpc.clone());
                    tokio::spawn(
                        db.insert(&data)
                          .map_err(|e| warn!("{:?}", e))
                          .and_then(move |_| {
                              rpc.storage(
                                  sender,
                                  StorageKey(storage_key.to_vec()),
                                  None,
                                  StorageKeyType::Timestamp(TimestampOp::Now)
                              ).map_err(|e| warn!("{:?}", e))
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

#[derive(Debug, PartialEq, Eq)]
struct Sync {
    looped: usize,
    blocks_missing: usize
}

impl std::fmt::Display for Sync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.blocks_missing)
    }
}

impl Sync {
    fn new() -> Self {
        Self {
            looped: 0,
            blocks_missing: 0,
        }
    }

    fn sync<T>(self,
                 db: Arc<Database>,
                 rpc: Arc<Rpc<T>>,
                 sender: UnboundedSender<Data<T>>,
    ) -> impl Future<Item = (Self, bool), Error = ()> + 'static
        where T: System + std::fmt::Debug + 'static
    {

        let sender0 = sender.clone();
        let looped = self.looped;
        info!("Looped: {}", looped);
        db.query_missing_blocks()
          .and_then(move |blocks| {
              match sender0
                  .unbounded_send(Data::SyncProgress(blocks.len()))
                  .map_err(Into::into) {
                      Ok(()) => (),
                      Err(e) => return future::err(e)
                  }
              future::ok(
                  blocks
                      .into_iter()
                      .map(|b| NumberOrHex::Hex(U256::from(b)))
                      .collect::<Vec<NumberOrHex<T::BlockNumber>>>()
              )
          }).and_then(move |blocks| {
              let length = blocks.len();
              rpc.batch_block_from_number(blocks, sender)
                 .and_then(move |_| {
                     if length == 0 {
                         thread::sleep(time::Duration::from_millis(60_000));
                     }
                     let looped = looped + 1;
                     future::ok((Self {blocks_missing: length, looped}, false ))
                 })
          }).map_err(|e| error!("{:?}", e))
    }
}
