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
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    future, FutureExt, StreamExt, TryFutureExt,
};
use log::*;
use runtime_primitives::traits::Header;
use substrate_primitives::U256;
use substrate_rpc_primitives::number::NumberOrHex;
use tokio::runtime::Runtime;

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{
    database::Database,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{BatchBlock, Data, System},
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
        log::debug!("METADATA: {}", rpc.metadata());
        log::debug!("KEYS: {:?}", rpc.keys());
        // log::debug!("PROPERTIES: {:?}", rpc.properties());
        Ok(Self { rpc, db, runtime })
    }

    pub fn run(mut self) -> Result<(), ArchiveError> {
        let (sender, receiver) = mpsc::unbounded();
        let data_in = Self::handle_data(receiver, self.db.clone());
        let blocks = Self::blocks(self.rpc.clone(), sender.clone());
        // .map_err(|e| log::error!("{:?}", e));
        let sync = Self::sync(self.rpc.clone(), self.db.clone()).map_err(|e| error!("{:?}", e));
        let handle = self.runtime.spawn(sync);
        self.runtime.block_on(future::join(data_in, blocks));
        self.runtime.block_on(handle);
        log::info!("All Done");
        Ok(())
    }

    async fn blocks(rpc: Arc<Rpc<T>>, sender: UnboundedSender<Data<T>>) {
        match rpc.subscribe_blocks(sender).await {
            Ok(_) => (),
            Err(e) => error!("{:?}", e),
        };
    }

    /// Verification task that ensures all blocks are in the database
    async fn sync(rpc: Arc<Rpc<T>>, db: Arc<Database>) -> Result<(), ArchiveError> {
        'sync: loop {
            let (db, rpc) = (db.clone(), rpc.clone());
            let (sync, done) = Sync::default().sync(db.clone(), rpc.clone()).await?;
            if done {
                break 'sync;
            }
        }
        Ok(())
    }

    async fn handle_data(mut receiver: UnboundedReceiver<Data<T>>, db: Arc<Database>) {
        while let Some(data) = receiver.next().await {
            match data {
                Data::SyncProgress(missing_blocks) => {
                    println!("{} blocks missing", missing_blocks);
                }
                c => {
                    let db = db.clone();
                    let fut = async move || db.insert(c).map_err(|e| log::error!("{:?}", e)).await;
                    tokio::spawn(fut());
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Sync<T: System + Debug> {
    looped: usize,
    _marker: PhantomData<T>,
}

impl<T> Default for Sync<T>
where
    T: System + Debug,
{
    fn default() -> Self {
        Self {
            looped: 0,
            _marker: PhantomData,
        }
    }
}

impl<T> Sync<T>
where
    T: System + Debug,
{
    async fn sync(self, db: Arc<Database>, rpc: Arc<Rpc<T>>) -> Result<(Self, bool), ArchiveError>
where {
        let blocks_done = Self::blocks(db.clone(), rpc.clone()).await?;
        let state_done = Self::state(db.clone(), rpc.clone()).await?;

        let looped = self.looped + 1;
        log::info!("Looped: {}", looped);
        let done = blocks_done && state_done;
        Ok((
            Self {
                looped,
                _marker: PhantomData,
            },
            done,
        ))
    }

    /// Crawl all state
    async fn state(db: Arc<Database>, rpc: Arc<Rpc<T>>) -> Result<bool, ArchiveError> {
        Ok(true)
    }

    async fn blocks(db: Arc<Database>, rpc: Arc<Rpc<T>>) -> Result<bool, ArchiveError> {
        let latest = rpc.clone().latest_block().await?;
        log::debug!("Latest Block: {:?}", latest);
        let latest = *latest
            .expect("should always be a latest; qed")
            .block
            .header
            .number();

        let blocks = db.query_missing_blocks(Some(latest.into())).await?;
        let mut futures = Vec::new();
        log::info!("Fetching {} blocks from rpc", blocks.len());
        let rpc0 = rpc.clone();
        for chunk in blocks.chunks(100_000) {
            let b = chunk
                .iter()
                .map(|b| NumberOrHex::Hex(U256::from(*b)))
                .collect::<Vec<NumberOrHex<T::BlockNumber>>>();
            futures.push(rpc.batch_block_from_number(b));
        }

        let mut blocks = Vec::new();
        for chunk in future::join_all(futures).await.into_iter() {
            blocks.extend(chunk?.into_iter());
        }

        log::info!("inserting {} blocks", blocks.len());
        let len = blocks.len();
        let b = db
            .insert(Data::BatchBlock(BatchBlock::<T>::new(blocks)))
            .await?;
        Ok(len == 0)
    }
}
