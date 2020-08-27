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
//! Messages that return nothing but an error may be sent to an asyncronous pool of actors
//! if state is an actor may be pulled out of the pool

use futures::{
    future::{Future, FutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use std::collections::VecDeque;
use std::pin::Pin;
use xtra::prelude::*;
use xtra::{Disconnected, WeakAddress};

// TODO: Could restart actors which have panicked
// TODO: If an actor disconnects remove it from the queue
/// A pool of one type of Actor
/// will distribute work to all actors in the pool
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

    /// Gets a weak address to the lastly-queued actor in the pool
    /// This actor will still receive messages that are sent to the pool
    /// and is not taken out. WeakAddresses can be used to
    /// communicate directly with a single actor.
    ///
    /// This has the possiblity of interrupting the pooled actors
    /// if many tasks are sent to the one actor.
    ///
    /// # None
    /// Returns none if there are no actors left in the pool
    #[allow(unused)]
    pub fn pull_weak(&self) -> Option<WeakAddress<A>> {
        self.queue.back().map(|a| a.downgrade())
    }

    /// Forward a message to one of the spawned actors
    /// and advance the state of the futures in queue.
    pub fn forward<M>(
        &mut self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = M::Result> + Send + 'static>>
    where
        M: Message + std::fmt::Debug + Send,
        M::Result: std::fmt::Debug + Unpin + Send,
        A: Handler<M>,
    {
        self.queue.rotate_left(1);
        spawn(self.queue[0].send(msg))
    }
}

fn spawn<R>(
    fut: impl Future<Output = Result<R, Disconnected>> + Send + 'static,
) -> Pin<Box<dyn Future<Output = R> + Send + 'static>>
where
    R: Send + 'static + Unpin + std::fmt::Debug,
{
    // flume isn't used here because it doesn't allow sending across `await` bound after
    // a move semantic (this might be a bug but i'm not sure), since every Sender includes a not
    // `Sync` Cell<bool>
    let (mut tx, mut rx) = futures::channel::mpsc::channel(0);

    let handle = smol::Task::spawn(async move { rx.next().await.map(|r: R| r).expect("One Shot") });
    let fut = async move {
        match fut.await {
            Ok(v) => {
                let _ = tx.send(v).await;
            }
            Err(_) => {
                log::error!(
                    "One of the pooled db actors has disconnected. could not send message."
                );
            }
        };
    };
    smol::Task::spawn(fut).detach();
    handle.boxed()
}

impl<A: Actor> Actor for ActorPool<A> {}

// We need a concrete struct for this otherwise our handler implementation
// conflicts with xtra's generic implementation for all T
pub struct PoolMessage<M: Message + Send>(pub M);

impl<M> Message for PoolMessage<M>
where
    M: Message + Send + Unpin + std::fmt::Debug,
{
    type Result = Pin<Box<dyn Future<Output = M::Result> + Send + 'static>>;
}

#[async_trait::async_trait]
impl<A, M> Handler<PoolMessage<M>> for ActorPool<A>
where
    A: Actor + Send + Clone + Handler<M>,
    M: Message + Send + std::fmt::Debug + Unpin,
    M::Result: Unpin + std::fmt::Debug,
{
    async fn handle(
        &mut self,
        msg: PoolMessage<M>,
        _: &mut Context<Self>,
    ) -> Pin<Box<dyn Future<Output = M::Result> + Send + 'static>> {
        self.forward(msg.0)
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
