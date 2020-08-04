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

/// An actor which manages a ThreadPool. Saturates a threadpool with items it receives, and sends
/// them to the address provided. Buffers items into the threadpool queue according to some
/// specified max size.
// on_started
//      setup postgres notification on the blocks table.
//      Spawn New Future
//          Send block number of the new insert to this actor.
//      Spawn New Future
//          Tries to send
//  on_block_number
//      Inserts block number into queue
//
use super::{ActorPool, DatabaseActor, GetState};
use crate::sql_block_builder::BlockBuilder;
use crate::{
    backend::BlockChanges,
    types::{Block, ThreadPool},
};
use sp_runtime::traits::Block as BlockT;
use std::fmt::Debug;
use std::sync::Arc;
use xtra::prelude::*;

pub struct BlockExecQueue<B: BlockT> {
    pool: Arc<dyn ThreadPool<In = Block<B>, Out = BlockChanges<B>> + 'static>,
    tx: flume::Sender<Block<B>>,
    rx: Option<flume::Receiver<Block<B>>>,
    /// internal buffer holding all items that need to be built
    queue: Vec<u32>,
    /// Address to send outputs to
    db: Address<ActorPool<DatabaseActor<B>>>,
}

impl<B: BlockT> BlockExecQueue<B> {
    pub fn new(
        pool: impl ThreadPool<In = Block<B>, Out = BlockChanges<B>> + 'static,
        db: Address<ActorPool<DatabaseActor<B>>>,
        size: usize,
    ) -> Self {
        let (tx, rx) = flume::bounded(size);
        let queue = Vec::new();

        Self {
            pool: Arc::new(pool),
            tx,
            rx: Some(rx),
            queue,
            db,
        }
    }
}

impl<B: BlockT> Actor for BlockExecQueue<B> {
    fn started(&mut self, ctx: &mut Context<Self>) {
        let mut rx = self.rx.take().unwrap();
        let pool = self.pool.clone();
        let addr = ctx.address().expect("Actor just started");
        let _handle = smol::Task::spawn(async move {
            let (work_tx, mut work_rx) = flume::unbounded();
            for change in work_rx.recv_async().await {
                let new_work = rx.recv_async().await.unwrap();
                pool.add_task(vec![new_work], work_tx.clone()).unwrap();
                if let Err(_) = addr.send(change).await {
                    break;
                }
            }
        });

        println!("Hello");
        // setup postgres notification on the blocks table
        // Block Number of the new insert to this actor
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {}
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<BlockChanges<B>> for BlockExecQueue<B> {
    async fn handle(&mut self, changes: BlockChanges<B>, ctx: &mut Context<Self>) {
        println!("Fuck yeah");
    }
}

pub struct In(pub u32);
impl Message for In {
    type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<In> for BlockExecQueue<B> {
    async fn handle(&mut self, num: In, ctx: &mut Context<Self>) {
        println!("Hello");
    }
}

pub struct BatchIn(pub Vec<u32>);
impl Message for BatchIn {
    type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT> Handler<BatchIn> for BlockExecQueue<B> {
    async fn handle(&mut self, incoming: BatchIn, ctx: &mut Context<Self>) {
        println!("Hello");
    }
}
