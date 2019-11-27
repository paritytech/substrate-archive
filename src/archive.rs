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
use async_stream::try_stream;
use futures::{
    channel::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    executor,
    future,
    task::{Context, Poll, Waker},
    Future, FutureExt, Stream, StreamExt,
};
use runtime_primitives::traits::Header;
use substrate_primitives::U256;
use substrate_rpc_primitives::number::NumberOrHex;

use std::{marker::PhantomData, sync::Arc, thread, time};

use core::pin::Pin;

use crate::{
    database::Database,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{BatchBlock, Data, SubstrateBlock, System},
};

// with the hopeful and long-anticipated release of async-await
pub struct Archive<T: System> {
    rpc: Arc<Rpc<T>>,
    db: Arc<Database>,
}

impl<T> Archive<T>
where
    T: System,
{
    pub fn new() -> Result<Self, ArchiveError> {
        let rpc = executor::block_on(Rpc::<T>::new(url::Url::parse("ws://127.0.0.1:9944")?))?;
        let db = Database::new()?;
        let (rpc, db) = (Arc::new(rpc), Arc::new(db));
        log::debug!("METADATA: {}", rpc.metadata());
        log::debug!("KEYS: {:?}", rpc.keys());
        // log::debug!("PROPERTIES: {:?}", rpc.properties());
        Ok(Self { rpc, db })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        let data_in = Self::handle_data(receiver, self.db.clone());
        let blocks = self
            .rpc
            .clone()
            .subscribe_blocks(sender.clone())
            .map_err(|e| log::error!("{:?}", e));

        self.runtime
            .spawn(Self::sync(self.rpc.clone(), self.db.clone()).map(|_| ()));
        tokio::run(blocks.join(data_in).map(|_| ()));
        Ok(())
    }

    async fn blocks(rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        match rpc.subscribe_blocks(sender).await {
            Ok(_) => (),
            Err(e) => error!("{:?}", e),
        };
    }

    /// Verification task that ensures all blocks are in the database
    fn sync(rpc: Arc<Rpc<T>>, db: Arc<Database>) -> impl Future<Item = Sync, Error = ()> {
        loop_fn(Sync::new(), move |v| {
            let (db, rpc) = (db.clone(), rpc.clone());
            rpc.clone()
                .latest_block()
                .map_err(|e| log::error!("{:?}", e))
                .map(move |latest| {
                    log::debug!("Latest Block: {:?}", latest);
                    *latest
                        .expect("should always be a latest; qed")
                        .block
                        .header
                        .number()
                })
                .and_then(move |latest| {
                    v.sync(db.clone(), latest.into(), rpc.clone())
                        .and_then(move |(sync, done)| {
                            if done {
                                Ok(Loop::Break(sync))
                            } else {
                                Ok(Loop::Continue(sync))
                            }
                        })
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
                    tokio::spawn(db.insert(c).map_err(|e| log::error!("{:?}", e)).map(|_| ()));
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

impl<T> Stream for Sync<T>
where
    T: System + std::fmt::Debug,
{
    type Item = Result<SyncStreamItem, ArchiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inner.poll_next(cx)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Sync {
    looped: usize,
    missing: usize, // missing timestamps + blocks
}

struct Sync<T: System + std::fmt::Debug> {
    // pub looped: usize,
    inner: Receiver<Result<SyncStreamItem, ArchiveError>>,
    timestamps_missing: usize,
    blocks_missing: usize,
}

impl<T> Sync<T>
where
    T: System + std::fmt::Debug,
{
    async fn new(
        db: Arc<Database>,
        rpc: Arc<Rpc<T>>,
        sender: UnboundedSender<Data<T>>,
    ) -> impl Stream<Item = Result<SyncStreamItem, ArchiveError>> {
        let (stream_sender, stream_receiver) = mpsc::channel(3);
        task::spawn(CrawlItems::new(db.clone(), rpc.clone(), sender.clone()).crawl(stream_sender));
        Self {
            looped: 0,
            missing: 0,
        }
    }

    fn sync<T>(
        self,
        db: Arc<Database>,
        latest: u64,
        rpc: Arc<Rpc<T>>,
    ) -> impl Future<Item = (Self, bool), Error = ()> + 'static
    where
        T: System + std::fmt::Debug + 'static,
    {
        let rpc0 = rpc.clone();
        let looped = self.looped;
        log::info!("Looped: {}", looped);
        log::info!("latest: {}", latest);
        db.query_missing_blocks(Some(latest))
            .and_then(move |blocks| {
                let mut futures = Vec::new();
                log::info!("Fetching {} blocks from rpc", blocks.len());
                for chunk in blocks.chunks(100_000) {
                    futures.push({
                        let b = chunk
                            .iter()
                            .map(|b| NumberOrHex::Hex(U256::from(*b)))
                            .collect::<Vec<NumberOrHex<T::BlockNumber>>>();
                        rpc0.clone().batch_block_from_number(b)
                    });
                }
                future::join_all(futures)
            })
            .map_err(|e| log::error!("{:?}", e))
            .and_then(move |b| {
                let blocks = b
                    .into_iter()
                    .flat_map(|b_inner| b_inner.into_iter())
                    .collect::<Vec<SubstrateBlock<T>>>();
                log::info!("inserting {} blocks", blocks.len());
                let missing = blocks.len();
                let b = db
                    .insert(Data::BatchBlock(BatchBlock::<T>::new(blocks)))
                    .map_err(|e| log::error!("{:?}", e));

                b.join(future::ok(missing))
            })
            .map(move |(b, missing)| {
                let looped = looped + 1;
                log::info!("Inserted {} blocks", missing);
                let missing = b;
                let done = missing == 0;
                (Self { looped, missing }, done)
            })
    }
}
