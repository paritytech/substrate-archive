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

use crate::{
    actors::{generators::fill_storage, workers::BlockFetcher, ActorContext},
    backend::BlockChanges,
    error::{self, ArchiveResult},
    types::{BatchBlock, Block, Storage},
};
use crossbeam::channel;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::sync::Arc;
use tokio::{runtime, task};
use xtra::prelude::*;

/// how often to check threadpools for finished work (in milli-seconds)
pub const SYSTEM_TICK: u64 = 1000;

// channels are used to avoid putting mutex on a VecDeque
/// Actor that combines individual types into sequences
/// results in batch inserts into the database (better perf)
/// also easier telemetry/logging
/// Handles sending and receiving messages from threadpools
#[derive(Clone)]
pub struct Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    queues: Queues<B>,
    /// actor which inserts blocks into the database
    db_addr: Address<super::Database>,
    /// Actor which manages getting the runtime metadata for blocks
    /// and sending them to the database actor
    meta_addr: Address<super::Metadata>,
    /// General Context containing global shared state useful in Actors
    context: ActorContext<B>,
    /// Pooled Postgres Database Connections
    pool: sqlx::PgPool,
}

/// Internal struct representing a queue built around message-passing
/// Sending/Receiving ends of queues to send batches of data to actors
/// includes shutdown signal to end the System Loop
#[derive(Clone)]
struct Queues<B: BlockT> {
    /// channel for receiving a shutdown signal
    rx: channel::Receiver<()>,
    /// channel for sending a shutdown signal
    tx: channel::Sender<()>,
    /// sending end of an internal queue to send batches of storage to actors
    storage_queue: channel::Sender<BlockChanges<B>>,
    /// sending end of an internal queue to send batches of blocks to actors
    block_queue: channel::Sender<Block<B>>,
    /// receiving end of an internal queue to send batches of storage to actors
    storage_recv: channel::Receiver<BlockChanges<B>>,
    /// receiving end of an internal queue to send batches of blocks to actors
    block_recv: channel::Receiver<Block<B>>,
}

enum BlockOrStorage<B: BlockT> {
    Block(Block<B>),
    Storage(BlockChanges<B>),
}

impl<B> Queues<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn new() -> Self {
        let (storage_tx, storage_rx) = channel::unbounded();
        let (block_tx, block_rx) = channel::unbounded();
        let (tx, rx) = channel::bounded(0);
        Self {
            rx,
            tx,
            storage_queue: storage_tx,
            block_queue: block_tx,
            storage_recv: storage_rx,
            block_recv: block_rx,
        }
    }

    fn push_back(&self, t: BlockOrStorage<B>) -> ArchiveResult<()> {
        match t {
            BlockOrStorage::Block(b) => self.block_queue.send(b)?,
            BlockOrStorage::Storage(s) => self.storage_queue.send(s)?,
        }
        Ok(())
    }

    fn pop_iter(&self) -> (Vec<Storage<B>>, Vec<Block<B>>) {
        (
            self.storage_recv
                .try_iter()
                .map(Storage::from)
                .collect::<Vec<Storage<B>>>(),
            self.block_recv.try_iter().collect(),
        )
    }

    fn should_shutdown(&self) -> bool {
        match self.rx.try_recv() {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn shutdown(&self) -> ArchiveResult<()> {
        self.tx.send(())?;
        Ok(())
    }
}

impl<B> Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    NumberFor<B>: From<u32>,
{
    pub fn new(context: ActorContext<B>, pool: &sqlx::PgPool) -> Self {
        let url = context.rpc_url();
        let db_addr = super::Database::new(&pool).spawn();
        let meta_addr = super::Metadata::new(url.to_string(), &pool).spawn();
        let queues = Queues::new();

        Self {
            queues,
            context,
            db_addr,
            meta_addr,
            pool: pool.clone(),
        }
    }

    fn check_work(&self) -> ArchiveResult<Count> {
        let (changes, blocks) = self.queues.pop_iter();

        let s_count = if changes.len() > 0 {
            let count = changes.len();
            self.db_addr.do_send(super::msg::VecStorageWrap(changes))?;
            count
        } else {
            0
        };

        let b_count = if blocks.len() > 0 {
            let count = blocks.len();
            self.meta_addr.do_send(BatchBlock::new(blocks))?;
            count
        } else {
            0
        };

        Ok(Count(b_count, s_count))
    }

    async fn kill(self) -> ArchiveResult<()> {
        self.queues.shutdown()?;
        Ok(())
    }
}

impl<B: BlockT> Message for BlockChanges<B> {
    type Result = ArchiveResult<()>;
}

struct Count(usize, usize);

impl Message for Count {
    type Result = ();
}

impl<B> Actor for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn started(&mut self, ctx: &mut Context<Self>) {
        let (broker, pool) = (self.context.broker(), self.pool.clone());

        crate::util::spawn(fill_storage(pool.clone(), broker.clone()));

        let this = self.clone();
        let addr = ctx.address().expect("Just instantiated; qed").clone();
        crate::util::spawn(async move {
            loop {
                timer::Delay::new(std::time::Duration::from_millis(SYSTEM_TICK)).await;
                if let Err(_) = addr.do_send(this.check_work()?) {
                    break;
                }
            }
            Ok(())
        });
    }
}

impl<B> SyncHandler<BlockChanges<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, changes: BlockChanges<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        self.queues.push_back(BlockOrStorage::Storage(changes))
    }
}

impl<B> SyncHandler<Block<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, block: Block<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        self.queues.push_back(BlockOrStorage::Block(block))
    }
}

impl<B> SyncHandler<Count> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, counts: Count, _: &mut Context<Self>) -> () {
        let (blocks, storage) = (counts.0, counts.1);
        match (blocks, storage) {
            (0, 0) => (),
            (b, 0) => log::info!("Indexing Blocks {} bps", b),
            (0, s) => log::info!("Indexing Storage {} bps", s),
            (b, s) => log::info!("Indexing Blocks {} bps, Indexing Storage {} bps", b, s),
        }
    }
}
