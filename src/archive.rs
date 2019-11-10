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
// use tokio::runtime::Runtime;
use async_std::task;
use substrate_rpc_primitives::number::NumberOrHex;
use async_stream::try_stream;
use futures::{
    future,
    Stream, Future,
    FutureExt, StreamExt,
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender, Sender, Receiver},
    task::{Context, Waker, Poll},
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

use core::pin::Pin;

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
}

impl<T> Archive<T> where T: System {

    pub fn new() -> Result<Self, ArchiveError> {
        let rpc = Rpc::<T>::new(url::Url::parse("ws://127.0.0.1:9944")?);
        let db = Database::new()?;
        let (rpc, db) = (Arc::new(rpc), Arc::new(db));
        // let metadata = runtime.block_on(rpc.metadata())?;
        // debug!("METADATA: {:?}", metadata);
        Ok( Self { rpc, db })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        crate::util::init_logger(log::LevelFilter::Error); // TODO user should decide log strategy

        let blocks = task::Builder::new().name("block subscription".into()).spawn(
            Self::blocks(self.rpc.clone(), sender.clone())
        );

        let sync = task::Builder::new().name("sync".into()).spawn(
            Self::sync(self.db.clone(), self.rpc.clone(), sender.clone())
        );

        let data = task::Builder::new().name("data".into()).spawn(
            Self::handle_data(receiver, self.db.clone())
        );

        task::block_on(future::join_all(blocks, sync, data));
        Ok(())
    }

    async fn blocks(rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        match rpc.subscribe_blocks(sender).await {
            Ok(_) => (),
            Err(e) => error!("{:?}", e)
        };
    }

    /// Verification task that ensures all blocks are in the database
    async fn sync(db: Arc<Database>, rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>)
    {
        let stream = Sync::<T>::new(db.clone(), rpc.clone(), sender.clone()).await;
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
/*
impl<T> Stream for Sync<T> where T: System + std::fmt::Debug {
    type Item = Result<SyncStreamItem, ArchiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.inner.poll(cx) {
            Poll::Ready(v) => {
                let v = v.map(|f| {
                    SyncStreamItem {
                        blocks_missing: v.0,
                        timestamps_missing: v.1
                    }
                });
                Poll::Ready(Some(v))
            },
            Poll::Pending => Poll::Pending
        }
    }
}
*/

impl<T> Stream for Sync<T> where T: System + std::fmt::Debug {
    type Item = Result<SyncStreamItem, ArchiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inner.poll_next(cx)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStreamItem {
    blocks_missing: usize,
    timestamps_missing: usize
}

struct Sync<T: System + std::fmt::Debug> {
    // pub looped: usize,
    inner: Receiver<Result<SyncStreamItem, ArchiveError>>,
    timestamps_missing: usize,
    blocks_missing: usize,
}

impl<T> Sync<T> where T: System + std::fmt::Debug {

    async fn new(db: Arc<Database>,
                  rpc: Arc<Rpc<T>>,
                  sender: UnboundedSender<Data<T>>,
    ) -> impl Stream<Item = Result<SyncStreamItem, ArchiveError>> {

        let (stream_sender, stream_receiver) = mpsc::channel(3);
        task::spawn(CrawlItems::new(db.clone(), rpc.clone(), sender.clone()).crawl(stream_sender));
        Self {
            timestamps_missing: 0,
            blocks_missing: 0,
            inner: stream_receiver
        }
    }
}

/*
impl<T> Future for CrawlItems<T>
    where T: System + std::fmt::Debug
{
    type Output = Result<(usize, usize), ArchiveError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let fut = self.inner.take();
        match fut {
            Some(v) => {
                match v.poll(cx) {
                    Poll::Ready(v) => {
                        Poll::Ready(v)
                    },
                    Poll::Pending => Poll::Pending
                }
            },
            None => {
                self.crawl();
                self.inner.expect("None value set by crawl; qed").poll(cx)
            }
        }
    }
}*/

struct CrawlItems<T: System + std::fmt::Debug> {
    db: Arc<Database>,
    rpc: Arc<Rpc<T>>,
    sender: UnboundedSender<Data<T>>,
    // inner: Option<Pin<Box<dyn Future<Output = Result<(usize, usize), ArchiveError>> + Send>>>
}

impl<T> CrawlItems<T> where T: System + std::fmt::Debug {

    fn new(db: Arc<Database>, rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) -> Self {
        Self {
            db, rpc, sender,
        }
    }

    async fn crawl(&self, stream_sender: Sender<Result<SyncStreamItem, ArchiveError>>) {
        let (db, rpc, sender) =
            (self.db.clone(), self.rpc.clone(), self.sender.clone());
        loop {
            let item : Result<SyncStreamItem, ArchiveError>
                = self.start()
                .await
                .map(|v| {
                    SyncStreamItem {
                        blocks_missing: v.0,
                        timestamps_missing: v.1
                    }
                });
            match stream_sender.try_send(item) {
                Ok(_) => (),
                Err(e) => error!("Send Failed {:?}", e),
            }
        }
    }

    async fn start(&self) -> Result<(usize, usize), ArchiveError>
    {
        future::try_join(
            CrawlItems::sync_timestamps(self.db.clone(), self.rpc.clone(), self.sender.clone()),
            CrawlItems::sync_blocks(self.db.clone(), self.rpc.clone(), self.sender.clone())
        ).await
    }
    /// find missing timestamps and add them to DB if found. Return number missing timestamps
    async fn sync_timestamps(db: Arc<Database>,
                             rpc: Arc<Rpc<T>>,
                             sender: UnboundedSender<Data<T>>
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
    async fn sync_blocks(db: Arc<Database>, rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>
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

