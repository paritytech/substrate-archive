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

use futures::{
    future::{self, loop_fn, Loop},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    Future, Stream,
};
use log::*;
use substrate_primitives::{storage::StorageKey, twox_128, U256};
use substrate_rpc_primitives::number::NumberOrHex;
use tokio::runtime::Runtime;

use std::sync::Arc;

use crate::{
    database::Database,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{Data, System},
};

// with the hopeful and long-anticipated release of async-await
pub struct Archive<T: System> {
    rpc: Arc<Rpc<T>>,
    db: Arc<Database>,
    runtime: Runtime,
}

impl<T> Archive<T>
where
    T: System,
{
    pub fn new() -> Result<Self, ArchiveError> {
        let mut runtime = Runtime::new()?;
        let rpc = runtime.block_on(Rpc::<T>::new(url::Url::parse("ws://127.0.0.1:9944")?))?;
        let db = Database::new()?;
        let (rpc, db) = (Arc::new(rpc), Arc::new(db));
        debug!("METADATA: {}", rpc.metadata());
        debug!("KEYS: {:?}", rpc.keys());
        Ok(Self { rpc, db, runtime })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        // self.runtime.block_on(self.rpc.clone().all_storage(sender.clone()))?;
        self.runtime.spawn(
            self.rpc
                .clone()
                .subscribe_blocks(sender.clone())
                .map_err(|e| println!("{:?}", e)),
        );
        self.runtime
            .spawn(Self::sync(self.db.clone(), self.rpc.clone(), sender.clone()).map(|_| ()));
        tokio::run(Self::handle_data(receiver, self.db.clone()));
        Ok(())
    }

    // TODO return a float between 0 and 1 corresponding to percent of database that is up-to-date?
    /// Verification task that ensures all blocks are in the database
    fn sync(
        db: Arc<Database>,
        rpc: Arc<Rpc<T>>,
        sender: UnboundedSender<Data<T>>,
    ) -> impl Future<Item = Sync, Error = ()> + 'static {
        loop_fn(Sync::new(), move |v| {
            let sender0 = sender.clone();
            v.sync(db.clone(), rpc.clone(), sender0.clone())
                .and_then(move |(sync, done)| {
                    if done {
                        Ok(Loop::Break(sync))
                    } else {
                        Ok(Loop::Continue(sync))
                    }
                })
        })
    }

    fn handle_data(
        receiver: UnboundedReceiver<Data<T>>,
        db: Arc<Database>,
    ) -> impl Future<Item = (), Error = ()> + 'static {
        receiver.for_each(move |data| {
            match data {
                Data::SyncProgress(missing_blocks) => {
                    println!("{} blocks missing", missing_blocks);
                }
                c => {
                    tokio::spawn(db.insert(c).map_err(|e| error!("{:?}", e)));
                }
            };
            future::ok(())
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Sync {
    looped: usize,
}

impl Sync {
    fn new() -> Self {
        Self { looped: 0 }
    }

    fn sync<T>(
        self,
        db: Arc<Database>,
        rpc: Arc<Rpc<T>>,
        sender: UnboundedSender<Data<T>>,
    ) -> impl Future<Item = (Self, bool), Error = ()> + 'static
    where
        T: System + std::fmt::Debug + 'static,
    {
        let (sender0, sender1) = (sender.clone(), sender.clone());
        let (rpc0, rpc1) = (rpc.clone(), rpc.clone());
        let looped = self.looped;
        info!("Looped: {}", looped);

        let missing_blocks = db
            .query_missing_blocks()
            .and_then(move |blocks| {
                match sender0
                    .unbounded_send(Data::SyncProgress(blocks.len()))
                    .map_err(Into::into)
                {
                    Ok(()) => (),
                    Err(e) => return future::err(e),
                }
                future::ok(
                    blocks
                        .into_iter()
                        .take(100_000) // just do 100K blocks at a time
                        .map(|b| NumberOrHex::Hex(U256::from(b)))
                        .collect::<Vec<NumberOrHex<T::BlockNumber>>>(),
                )
            })
            .and_then(move |blocks| {
                rpc0.batch_block_from_number(blocks, sender)
                    .and_then(move |_| {
                        let looped = looped + 1;
                        future::ok((Self { looped }, false))
                    })
            });

        let missing_timestamps = db.query_missing_timestamps::<T>().and_then(move |hashes| {
            info!(
                "Launching timestamp insertion thread for {} items",
                hashes.len()
            );
            let timestamp_key = b"Timestamp Now";
            let storage_key = twox_128(timestamp_key);
            let keys = std::iter::repeat(StorageKey(storage_key.to_vec()))
                .take(hashes.len())
                .collect::<Vec<StorageKey>>();
            rpc1.batch_storage(sender1, keys, hashes)
        });
        missing_timestamps
            .join(missing_blocks)
            .map_err(|e| error!("{:?}", e))
            .map(|(_, b)| b)
    }
}
