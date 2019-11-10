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
use tokio::runtime::Runtime;
use substrate_rpc_primitives::number::NumberOrHex;
use async_stream::try_stream;
use futures::{
    Stream,
    FutureExt, StreamExt,
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use substrate_primitives::{
    U256,
    storage::StorageKey,
    twox_128
};

use std::{
    sync::Arc,
    marker::PhantomData,
    thread,
    time
};

use crate::{
    database::Database,
    rpc::Rpc,
    error::Error as ArchiveError,
    types::{System, Data, storage::{StorageKeyType, TimestampOp}},
};

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
        // let metadata = runtime.block_on(rpc.metadata())?;
        // debug!("METADATA: {:?}", metadata);
        Ok( Self { rpc, db, runtime })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        crate::util::init_logger(log::LevelFilter::Error); // TODO user should decide log strategy

        // block subscription thread
        self.runtime.spawn(Self::blocks(self.rpc.clone(), sender.clone()));

        // crawls database and retrieves missing values
        self.runtime.spawn(Self::sync(self.db.clone(), self.rpc.clone(), sender.clone()));

        // inserts into database / handles misc. data (ie sync progress, etc)
        self.runtime.block_on(Self::handle_data(receiver, self.db.clone()));

        Ok(())
    }

    async fn blocks(rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        match rpc.subscribe_blocks(sender).await {
            Ok(_) => (),
            Err(e) => error!("{:?}", e)
        };
    }

    /// Verification task that ensures all blocks are in the database
    async fn sync(db: Arc<Database>, rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        let stream = Sync::new().sync(db.clone(), rpc.clone(), sender.clone()).await;
        futures_util::pin_mut!(stream);
        while let Some(v) = stream.next().await {
            if v.is_err() {
                error!("{:?}", v);
            } else {
                let v = v. expect("Error is checked; qed");
                sender
                    .clone()
                    .unbounded_send(Data::SyncProgress(v.blocks_missing))
                    .map_err(|e| error!("{:?}", e));
            }
        }
    }

    async fn handle_data(receiver: UnboundedReceiver<Data<T>>, db: Arc<Database>) {

        for data in receiver.next().await {
            match data {
                Data::SyncProgress(missing_blocks) => {
                    println!("{} blocks missing", missing_blocks);
                },
                c @ _ => {
                    // possibly use tokio::spawn
                    db.insert(data).map(|v| {
                        if v.is_err() {
                            error!("{:?}", v);
                        }
                    }).await
                }
            };
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStreamItem {
    blocks_missing: usize,
    timestamps_missing: usize
}

#[derive(Debug, PartialEq, Eq)]
struct Sync<T: System + std::fmt::Debug> {
    last: usize,
    looped: usize,
    _marker: PhantomData<T>
}

impl<T> Sync<T> where T: System + std::fmt::Debug {

    fn new() -> Self {
        Self {
            last: 0,
            looped: 0,
            _marker: PhantomData
        }
    }

    async fn sync(self,
                  db: Arc<Database>,
                  rpc: Arc<Rpc<T>>,
                  sender: UnboundedSender<Data<T>>,
    ) -> impl Stream<Item = Result<SyncStreamItem, ArchiveError>> {

        info!("Looped: {}", &self.looped);
        try_stream! {
            loop {
                let timestamps_missing = self.sync_timestamps(db.clone(), rpc.clone(), sender.clone()).await?;
                let blocks_missing = self.sync_blocks(db.clone(), rpc.clone(), sender.clone()).await?;

                // sleep, this thread does not need to run all the time
                if blocks_missing == 0 && timestamps_missing == 0 {
                    // TODO change to non-blocking sleep
                    thread::sleep(time::Duration::from_millis(10_000));
                }
                yield SyncStreamItem { blocks_missing, timestamps_missing };
            }
        }
    }

    /// find missing timestamps and add them to DB if found. Return number missing timestamps
    async fn sync_timestamps(&self, db: Arc<Database>, rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>
    ) -> Result<usize, ArchiveError>
    {
        let hashes = db.query_missing_timestamps::<T>().await?;
        let num_missing = hashes.len();
        let timestamp_key = b"Timestamp Now";
        let storage_key = twox_128(timestamp_key);
        let keys = std::iter::repeat(StorageKey(storage_key.to_vec()))
            .take(hashes.len())
            .collect::<Vec<StorageKey>>();
        let key_types = std::iter::repeat(StorageKeyType::Timestamp(TimestampOp::Now))
            .take(hashes.len())
            .collect::<Vec<StorageKeyType>>();
        rpc.batch_storage(sender, keys, hashes, key_types).await?;
        Ok(num_missing)
    }

    /// sync blocks, 100,000 at a time, returning number of blocks that were missing before synced
    async fn sync_blocks(&self, db: Arc<Database>, rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>
    ) -> Result<usize, ArchiveError>
    {
        let missing_blocks = db.query_missing_blocks().await?;
        let blocks = missing_blocks
            .into_iter()
            .take(100_000)
            .map(|b| NumberOrHex::Hex(U256::from(b)))
            .collect::<Vec<NumberOrHex<T::BlockNumber>>>();
        rpc.batch_block_from_number(blocks, sender).await?;
        // sender.unbounded_send(Data::SyncProgress(blocks.len()));
        Ok(blocks.len())
    }
}
