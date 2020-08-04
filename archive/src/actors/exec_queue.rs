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

use crate::types::ThreadPool;
use xtra::prelude::*;
use std::fmt::Debug;
use std::sync::Arc;

type Pool<I, O> = Arc<dyn ThreadPool<In = I, Out = O>>;
pub struct BlockExecQueue<I, O, A> {
    pool: Pool<I, O>,
    tx: flume::Sender<I>,
    rx: Option<flume::Receiver<I>>,
    /// internal buffer holding all items that need to be built
    queue: Vec<I>,
    /// Address to send outputs to
    out_addr: Address<A>
}

impl<I, O, A> ExecQueue<I, O, A> 
where   
    I: Send + Sync,
    O: Send + Sync + Debug,
    A: Actor
{
    pub fn new(pool: Pool<I, O>, queue: Vec<I>, actor: Address<A>, size: usize) -> Self {
        let (tx, rx) = flume::bounded(size);
        let out_addr = Arc::new(actor);
        Self {
            pool: pool,
            tx,
            rx: Some(rx),
            queue,
            out_addr,
        }
    }
}

impl<I, O, A> Actor for ExecQueue<I, O, A> 
where
    I: Send + Sync + Debug,
    O: Send + Sync + Debug,
{
    fn started(&mut self, ctx: &mut Context<Self>) {
        let rx = self.rx.take().unwrap();
        let pool = self.pool.clone();
        let addr = ctx.address().expect("Actor just started");
        let _handle = smol::Task::spawn(async move {
            let (work_tx, work_rx) = flume::unbounded();
            for change in work_rx.recv_async().await {
                let new_work = rx.recv_async().await.unwrap();
                pool.add_task(vec![new_work], work_tx);
                if let Err(_) = addr.send(Out(change)).await {
                    break;
                }
            }
        });

        println!("Hello"); 
        // setup postgres notification on the blocks table
        // Block Number of the new insert to this actor
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        
    }
}

struct Out<O: Send + Sync>(O);
impl<O: Send + Sync> Message for Out<O> {
    type  Result = ();
}

#[async_trait::async_trait]
impl<I, O, A> Handler<Out<O>> for ExecQueue<I, O, A> 
where
    I: Send + Sync + Debug,
    O: Send + Sync + Debug,
{
    async fn handle(&mut self, change: Out<O>, ctx: &mut Context<Self>) {
        println!("Fuck yeah"); 
    }
}

pub struct In<I: Send + Sync>(pub I);
impl<I: Send + Sync> Message for In<I> {
    type Result = ();
}

#[async_trait::async_trait]
impl<I, O, A> Handler<In<I>> for ExecQueue<I, O, A> 
where
    I:  Send + Sync + Debug,
    O: Send + Sync + Debug 
{
    async fn handle(&mut self, incoming: In<I>, ctx: &mut Context<Self>) {
        println!("Hello");
    }
}

pub struct BatchIn<I: Send + Sync>(pub Vec<I>);
impl<I: Send + Sync> Message for BatchIn<I> {
    type Result = ();
}

#[async_trait::async_trait]
impl<I, O, A> Handler<BatchIn<I>> for ExecQueue<I, O, A> 
where
    I: Send + Sync + Debug,
    O: Send + Sync + Debug, 
{
    async fn handle(&mut self, incoming: BatchIn<I>, ctx: &mut Context<Self>) {
        println!("Hello");
    }
}

