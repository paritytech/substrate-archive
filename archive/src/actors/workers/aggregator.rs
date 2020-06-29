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
    actors::{
        generators::{fill_storage, missing_blocks},
        workers::BlockFetcher,
        ActorContext,
    },
    backend::BlockChanges,
    error::ArchiveResult,
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
    /// the Actor which manages sending blocks to be fetched by the RocksDB backend
    /// in a threadpool
    fetch: Option<Address<BlockFetcher<B>>>,
    /// General Context containing global shared state useful in Actors
    context: ActorContext<B>,
    /// the tokio context this actor is running in
    handle: runtime::Handle,
    /// the handle to the core system loop
    core_loop: task::JoinHandle<ArchiveResult<()>>,
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
    pub fn new(context: ActorContext<B>, handle: runtime::Handle) -> Self {
        let url = context.rpc_url();
        let pool = context.pool();
        let db_addr = super::Database::new(&pool).spawn();
        let meta_addr = super::Metadata::new(url.to_string(), &pool).spawn();
        let queues = Queues::new();
        let core_loop = handle.spawn(Self::aggregate(
            queues.clone(),
            meta_addr.clone(),
            db_addr.clone(),
        ));
        Self {
            queues,
            fetch: None,
            context,
            db_addr,
            meta_addr,
            handle,
            core_loop,
        }
    }

    async fn aggregate(
        queues: Queues<B>,
        meta: Address<super::Metadata>,
        db: Address<super::Database>,
    ) -> ArchiveResult<()> {
        loop {
            let (blocks, storage) = Self::check_work(&queues, &meta, &db)?;
            match (blocks, storage) {
                (0, 0) => (),
                (b, 0) => log::info!("Indexing Blocks {} bps", b),
                (0, s) => log::info!("Indexing Storage {} bps", s),
                (b, s) => log::info!("Indexing Blocks {} bps, Indexing Storage {} bps", b, s),
            }
            timer::Delay::new(std::time::Duration::from_millis(SYSTEM_TICK)).await;
            if queues.should_shutdown() {
                break;
            }
        }
        Ok(())
    }

    fn check_work(
        queues: &Queues<B>,
        meta: &Address<super::Metadata>,
        db: &Address<super::Database>,
    ) -> ArchiveResult<(usize, usize)> {
        let (changes, blocks) = queues.pop_iter();

        let s_count = if changes.len() > 0 {
            let count = changes.len();
            // FIXME: unwrap
            db.do_send(super::msg::VecStorageWrap(changes)).unwrap();
            count
        } else {
            0
        };

        let b_count = if blocks.len() > 0 {
            let count = blocks.len();
            // FIXME unwrap
            meta.do_send(BatchBlock::new(blocks)).unwrap();
            count
        } else {
            0
        };

        Ok((s_count, b_count))
    }

    async fn kill(self) -> ArchiveResult<()> {
        self.queues.shutdown()?;
        // FIXME error
        self.core_loop
            .await
            .expect("Core loop failed to execute to completion");
        Ok(())
    }
}

impl<B: BlockT> Message for BlockChanges<B> {
    type Result = ArchiveResult<()>;
}

impl<B> Actor for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn started(&mut self, ctx: &mut Context<Self>) {
        let (broker, pool) = (self.context.broker(), self.context.pool());

        let this_addr = ctx.address().expect("Actor just started");
        let (fetch, mgr) = match super::BlockFetcher::new(self.context.clone(), this_addr, Some(3))
        {
            Ok(v) => v.create(),
            Err(e) => {
                log::error!("{}", e);
                panic!("Could not start Block Fetcher ThreadPool. Exiting.")
            }
        };
        self.handle
            .spawn(fill_storage(pool.clone(), broker.clone()));
        self.handle.spawn(missing_blocks(pool, fetch.clone()));

        self.handle.spawn(mgr.manage());
        self.fetch = Some(fetch);
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        // drop the fblock fetcher actor
        self.fetch = None;
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

pub struct Head<B: BlockT>(pub B::Header);

impl<B: BlockT> Message for Head<B> {
    type Result = ArchiveResult<()>;
}

impl<B> SyncHandler<Head<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, head: Head<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        self.fetch.as_ref().map(|f| f.do_send(head));
        Ok(())
    }
}
