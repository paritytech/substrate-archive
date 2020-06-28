// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

//! Work generated from PostgreSQL database
//! Crawls for missing entires in the storage table
//! Gets storage changes based on those missing entries

// TODO: Can create a trait "ExtraAddressExt" that allows attaching a crossbeam stream, or a jsonrpsee
// Subscription

use crate::{
    actors::{
        workers::{self, msg::VecStorageWrap},
        ActorContext,
    },
    backend::{BlockBroker, BlockData},
    database::Database,
    error::ArchiveResult,
    queries,
    sql_block_builder::BlockBuilder,
    types::Storage,
};
use crossbeam::channel::{Receiver, Sender};
use sp_runtime::traits::{Block as BlockT, NumberFor};
use xtra::prelude::*;

pub struct MissingStorage<Block: BlockT> {
    context: ActorContext<Block>,
    addr: Address<Database>,
    tx: Sender<()>,
    handle: tokio::task::JoinHandle<ArchiveResult<()>>,
}

impl<Block: BlockT> Actor for MissingStorage<Block> {}

impl<Block> MissingStorage<Block>
where
    Block: BlockT,
    NumberFor<Block>: Into<u32>,
{
    /// create a new MissingStorage Indexer
    /// Must be run within the context of an executor
    pub fn new(context: ActorContext<Block>) -> ArchiveResult<Self> {
        let (tx, rx) = crossbeam::channel::bounded(0);
        let addr = Database::new(&context.pool()).spawn();
        let handle = tokio::spawn(Self::storage_loop(context.clone(), addr.clone(), rx));

        Ok(Self {
            context,
            addr,
            tx,
            handle,
        })
    }

    pub async fn stop(self) -> ArchiveResult<()> {
        self.tx.send(()).expect("Send is infallible");
        self.handle.await.expect("Should not fail");
        Ok(())
    }

    async fn storage_loop(
        context: ActorContext<Block>,
        addr: Address<Database>,
        rx: Receiver<()>,
    ) -> ArchiveResult<()> {
        Self::on_start(&context).await?;
        Self::main_loop(context, addr, rx).await?;
        Ok(())
    }

    async fn on_start(context: &ActorContext<Block>) -> ArchiveResult<()> {
        if queries::blocks_count(&context.pool()).await? <= 0 {
            // no blocks means we haven't indexed anything yet
            return Ok(());
        }
        let now = std::time::Instant::now();
        let blocks = queries::blocks_storage_intersection(&context.pool()).await?;
        let blocks = BlockBuilder::new().with_vec(blocks)?;
        let elapsed = now.elapsed();
        log::info!(
            "TOOK {} seconds, {} milli-seconds to get and build {} blocks",
            elapsed.as_secs(),
            elapsed.as_millis(),
            blocks.len()
        );
        log::info!("indexing {} blocks of storage ... ", blocks.len());
        context.broker().work.send(BlockData::Batch(blocks)).unwrap();
        Ok(())
    }

    async fn main_loop(
        context: ActorContext<Block>,
        addr: Address<Database>,
        rx: Receiver<()>,
    ) -> ArchiveResult<()> {
        loop {
            timer::Delay::new(std::time::Duration::from_secs(1)).await;
            let count = Self::check_work(&context, &addr)?;
            if count > 0 {
                log::info!("Syncing Storage {} bps", count);
            }
            match rx.try_recv() {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
        Ok(())
    }

    /// Check the receiver end of the BlockExecution ThreadPool for any storage
    /// changes resulting from block execution.
    fn check_work(context: &ActorContext<Block>, addr: &Address<Database>) -> ArchiveResult<usize> {
        let changes = context
            .broker
            .results
            .try_iter()
            .map(|c| Storage::<Block>::from(c))
            .collect::<Vec<Storage<Block>>>();

        if changes.len() > 0 {
            let count = changes.len();
            addr.do_send(VecStorageWrap(changes)).unwrap();
            Ok(count)
        } else {
            Ok(0)
        }
    }
}
