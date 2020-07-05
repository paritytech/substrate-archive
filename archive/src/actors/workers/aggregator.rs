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
    backend::BlockChanges,
    error::ArchiveResult,
    threadpools::BlockData,
    types::{BatchBlock, Block, Storage},
};
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
    db_addr: Address<super::Database>,
    /// Actor which manages getting the runtime metadata for blocks
    /// and sending them to the database actor
    meta_addr: Address<super::Metadata>,
    /// Pooled Postgres Database Connections
    exec: flume::Sender<BlockData<B>>,
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
    storage_queue: flume::Sender<BlockChanges<B>>,
    /// sending end of an internal queue to send batches of blocks to actors
    block_queue: flume::Sender<Block<B>>,
}

struct Receivers<B: BlockT> {
    /// receiving end of an internal queue to send batches of storage to actors
    storage_recv: flume::Receiver<BlockChanges<B>>,
    /// receiving end of an internal queue to send batches of blocks to actors
    block_recv: flume::Receiver<Block<B>>,
}

impl<B> Receivers<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn check_work(&self) -> BlockStorageCombo<B> {
        self.storage_recv
            .drain()
            .map(Storage::from)
            .zip_longest(self.block_recv.drain())
            .collect::<BlockStorageCombo<B>>()
    }
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
    pub fn new(url: &str, tx: flume::Sender<BlockData<B>>, pool: &sqlx::PgPool) -> Self {
        let db_addr = super::Database::new(&pool).spawn();
        let meta_addr = super::Metadata::new(url.to_string(), &pool).spawn();
        let (senders, recvs) = queues();

        Self {
            senders,
            recvs: Some(recvs),
            db_addr,
            meta_addr,
            exec: tx,
        }
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
            this.check_work()
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
    fn handle(&mut self, block: Block<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        self.exec.send(BlockData::Single(block.clone()))?;
        self.senders.push_back(BlockOrStorage::Block(block))
    }
}

impl<B> SyncHandler<BatchBlock<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, blocks: BatchBlock<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        self.exec.send(BlockData::Batch(blocks.inner.clone()))?;
        self.senders.push_back(BlockOrStorage::BatchBlock(blocks))
    }
}

struct BlockStorageCombo<B: BlockT>(BatchBlock<B>, super::msg::VecStorageWrap<B>);

impl<B: BlockT> Message for BlockStorageCombo<B> {
    type Result = ArchiveResult<()>;
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

impl<B> SyncHandler<BlockStorageCombo<B>> for Aggregator<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn handle(&mut self, counts: BlockStorageCombo<B>, _: &mut Context<Self>) -> ArchiveResult<()> {
        let (blocks, storage) = (counts.0, counts.1);

        let s_count = if !storage.0.is_empty() {
            let count = storage.0.len();
            self.db_addr.do_send(storage)?;
            count
        } else {
            0
        };

        let b_count = if !blocks.inner().is_empty() {
            let count = blocks.inner().len();
            self.meta_addr.do_send(blocks)?;
            count
        } else {
            0
        };

        match (b_count, s_count) {
            (0, 0) => (),
            (b, 0) => log::info!("Indexing Blocks {} bps", b),
            (0, s) => log::info!("Indexing Storage {} bps", s),
            (b, s) => log::info!("Indexing Blocks {} bps, Indexing Storage {} bps", b, s),
        };
        Ok(())
    }
}
