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

//! A module that handles a pool of actors
use crate::error::ArchiveResult;
use futures::future::Future;
use std::collections::VecDeque;
use xtra::prelude::*;
use xtra::Disconnected;

pub struct ActorPool<A: Actor> {
    queue: VecDeque<Address<A>>,
    pure_actor: A,
}

impl<A: Actor + Send + Clone> ActorPool<A> {
    /// creates a new actor pool of size `size`
    pub fn new(actor: A, size: usize) -> Self {
        let mut queue = VecDeque::with_capacity(size);
        for _ in 0..size {
            let a = actor.clone();
            queue.push_back(a.spawn())
        }

        Self {
            queue,
            pure_actor: actor,
        }
    }

    /// grow the pool by `n` actors
    #[allow(unused)]
    pub fn grow(&mut self, n: usize) {
        self.queue.reserve(n);
        for _ in 0..n {
            let a = self.pure_actor.clone();
            self.queue.push_back(a.spawn())
        }
    }

    /// shrink the pool by `n` actors
    #[allow(unused)]
    pub fn shrink(&mut self, n: usize) {
        for _ in 0..n {
            let a = self.queue.pop_front();
            // dropping the actor will cause it to call its stop() method
            // once it realizes no strong addresses hold it any longer
            std::mem::drop(a);
        }
    }

    /// Forward a message to one of the spawned actors
    /// and advance the state of the futures in queue.
    pub async fn forward<M>(&mut self, msg: M)
    where
        M: Message<Result = ArchiveResult<()>>,
        A: Handler<M>,
    {
        self.queue.rotate_left(1);
        crate::util::spawn(spawn(self.queue[0].send(msg)));
    }
}

async fn spawn(
    fut: impl Future<Output = Result<ArchiveResult<()>, Disconnected>>,
) -> ArchiveResult<()> {
    match fut.await {
        Ok(v) => v,
        Err(_) => {
            log::error!("one of the pooled db actors has disconnected");
            //TODO: Panic?
            Ok(())
        }
    }
}

impl<A: Actor> Actor for ActorPool<A> {}

// We need a concrete struct for this otherwise our handler implementation
// conflicts with xtra's generic implementation for all T
pub struct PoolMessage<M: Message + Send>(M);

impl<M> Message for PoolMessage<M>
where
    M: Message + Send,
{
    type Result = ();
}

#[async_trait::async_trait]
impl<A, M> Handler<PoolMessage<M>> for ActorPool<A>
where
    A: Actor + Send + Clone + Handler<M>,
    M: Message<Result = ArchiveResult<()>> + Send,
{
    async fn handle(&mut self, msg: PoolMessage<M>, _: &mut Context<Self>) {
        self.forward(msg.0).await;
    }
}

impl<M> From<M> for PoolMessage<M>
where
    M: Message + Send,
{
    fn from(m: M) -> PoolMessage<M> {
        PoolMessage(m)
    }
}
