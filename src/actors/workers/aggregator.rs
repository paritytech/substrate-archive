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

use super::ActorContext;
use crate::{
    actors::Die,
    backend::BlockChanges,
    error::ArchiveResult,
    threadpools::BlockData,
    types::{BatchBlock, Block, Storage},
};
use flume::Sender;
use futures::future::Either;
use itertools::{EitherOrBoth, Itertools};
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::{iter::FromIterator, time::Duration};
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
    senders: Senders<B>,
    recvs: Option<Receivers<B>>,
    /// actor which inserts blocks into the database
    db_pool: Address<super::ActorPool<super::DatabaseActor<B>>>,
    /// Actor which manages getting the runtime metadata for blocks
    /// and sending them to the database actor
    meta_addr: Address<super::Metadata<B>>,
    /// channel for sending blocks to be executed
    exec: Sender<BlockData<B>>,
    /// just a switch so we know not to print redundant messages
    last_count_was_0: bool,
}

fn queues<B>() -> (Senders<B>, Receivers<B>)
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    let (storage_tx, storage_rx) = flume::unbounded();
    let (block_tx, block_rx) = flume::unbounded();
    (
        Senders {
            storage_queue: storage_tx,
            block_queue: block_tx,
        },
        Receivers {
            storage_recv: storage_rx,
            block_recv: block_rx,
        },
    )
}

/// Internal struct representing a queue built around message-passing
/// Sending/Receiving ends of queues to send batches of data to actors
struct Senders<B: BlockT> {
    /// sending end of an internal queue to send batches of storage to actors
    storage_queue: Sender<BlockChanges<B>>,
    /// sending end of an internal queue to send batches of blocks to actors
    block_queue: Sender<Block<B>>,
}

struct Receivers<B: BlockT> {
    /// receiving end of an internal queue to send batches of storage to actors
    storage_recv: flume::Receiver<BlockChanges<B>>,
    /// receiving end of an internal queue to send batches of blocks to actors
    block_recv: flume::Receiver<Block<B>>,
}

enum BlockOrStorage<B: BlockT> {
    Block(Block<B>),
    BatchBlock(BatchBlock<B>),
    Storage(BlockChanges<B>),
}

impl<B> Senders<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn push_back(&self, t: BlockOrStorage<B>) -> ArchiveResult<()> {
        match t {
            BlockOrStorage::Block(b) => self.block_queue.send(b)?,
            BlockOrStorage::Storage(s) => self.storage_queue.send(s)?,
            BlockOrStorage::BatchBlock(v) => {
                for b in v.inner.into_iter() {
                    self.block_queue.send(b)?;
                }
            }
        }
        Ok(())
    }
}

impl<B> Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    NumberFor<B>: From<u32>,
{
    pub async fn new(
        ctx: ActorContext<B>,
        tx_block: Sender<BlockData<B>>,
        tx_num: Sender<u32>,
    ) -> ArchiveResult<Self> {
        let (psql_url, rpc_url) = (ctx.psql_url().to_string(), ctx.rpc_url().to_string());
        let db = super::DatabaseActor::new(psql_url).await?;
        let db_pool = super::ActorPool::new(db, 4).spawn();
        let meta_addr = super::Metadata::new(rpc_url, db_pool.clone())
            .await?
            .spawn();
        super::Generator::new(db_pool.clone(), tx_block.clone(), tx_num).start()?;
        let (senders, recvs) = queues();

        Ok(Self {
            senders,
            db_pool,
            recvs: Some(recvs),
            meta_addr,
            exec: tx_block,
            last_count_was_0: false,
        })
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
        if self.recvs.is_none() {
            let (sends, recvs) = queues();
            self.senders = sends;
            self.recvs = Some(recvs);
        }
        let this = self.recvs.take().expect("checked for none; qed");
        ctx.notify_interval(Duration::from_millis(SYSTEM_TICK), move || {
            this.storage_recv
                .drain()
                .map(Storage::from)
                .zip_longest(this.block_recv.drain())
                .collect::<BlockStorageCombo<B>>()
        });
    }
}

impl<B> SyncHandler<BlockChanges<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, changes: BlockChanges<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        self.senders.push_back(BlockOrStorage::Storage(changes))
    }
}

impl<B> SyncHandler<Block<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, block: Block<B>, c: &mut Context<Self>) {
        if let Err(_) = self.exec.send(BlockData::Single(block.clone())) {
            c.stop();
        }
        let block = BlockOrStorage::Block(block);
        if let Err(_) = self.senders.push_back(block) {
            c.stop();
        }
    }
}

impl<B> SyncHandler<BatchBlock<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, blocks: BatchBlock<B>, c: &mut Context<Self>) {
        if let Err(_) = self.exec.send(BlockData::Batch(blocks.inner.clone())) {
            c.stop();
        }
        let blocks = BlockOrStorage::BatchBlock(blocks);
        if let Err(_) = self.senders.push_back(blocks) {
            c.stop();
        }
    }
}

struct BlockStorageCombo<B: BlockT>(BatchBlock<B>, super::msg::VecStorageWrap<B>);

impl<B: BlockT> Message for BlockStorageCombo<B> {
    type Result = ();
}

impl<B: BlockT> FromIterator<EitherOrBoth<Storage<B>, Block<B>>> for BlockStorageCombo<B> {
    fn from_iter<I: IntoIterator<Item = EitherOrBoth<Storage<B>, Block<B>>>>(iter: I) -> Self {
        let mut storage = Vec::new();
        let mut blocks = Vec::new();
        for i in iter {
            match i {
                EitherOrBoth::Left(s) => storage.push(s),
                EitherOrBoth::Right(b) => blocks.push(b),
                EitherOrBoth::Both(s, b) => {
                    storage.push(s);
                    blocks.push(b);
                }
            }
        }
        BlockStorageCombo(BatchBlock::new(blocks), super::msg::VecStorageWrap(storage))
    }
}

#[async_trait::async_trait]
impl<B> Handler<BlockStorageCombo<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, data: BlockStorageCombo<B>, _: &mut Context<Self>) {
        let (blocks, storage) = (data.0, data.1);

        let (b, s) = (blocks.inner().len(), storage.0.len());
        match (b, s) {
            (0, 0) => {
                if !self.last_count_was_0 {
                    log::info!("Waiting on node, nothing left to index ...");
                    self.last_count_was_0 = true;
                }
            }
            (b, 0) => {
                self.meta_addr.do_send(blocks).expect("Actor Disconnected");
                log::info!("Indexing Blocks {} bps", b);
                self.last_count_was_0 = false;
            }
            (0, s) => {
                self.db_pool
                    .do_send(storage.into())
                    .expect("Actor Disconnected");
                log::info!("Indexing Storage {} bps", s);
                self.last_count_was_0 = false;
            }
            (b, s) => {
                self.db_pool
                    .do_send(storage.into())
                    .expect("Actor Disconnected");
                self.meta_addr.do_send(blocks).expect("Actor Disconnected");
                log::info!("Indexing Blocks {} bps, Indexing Storage {} bps", b, s);
                self.last_count_was_0 = false;
            }
        };
    }
}

pub struct IncomingData<B: BlockT>(Either<BlockChanges<B>, Block<B>>);

impl<B: BlockT> From<Either<BlockChanges<B>, Block<B>>> for IncomingData<B> {
    fn from(e: Either<BlockChanges<B>, Block<B>>) -> IncomingData<B> {
        IncomingData(e)
    }
}

impl<B: BlockT> Message for IncomingData<B> {
    type Result = ();
}

impl<B> SyncHandler<IncomingData<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, data: IncomingData<B>, c: &mut Context<Self>) {
        let r = || -> ArchiveResult<()> {
            match data.0 {
                Either::Left(changes) => self.senders.push_back(BlockOrStorage::Storage(changes)),
                Either::Right(block) => {
                    self.exec.send(BlockData::Single(block.clone()))?;
                    self.senders.push_back(BlockOrStorage::Block(block))
                }
            }
        };
        if let Err(_) = r() {
            c.stop()
        }
    }
}

impl<B> SyncHandler<Die> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, _: Die, c: &mut Context<Self>) -> ArchiveResult<()> {
        c.stop();
        Ok(())
    }
}
