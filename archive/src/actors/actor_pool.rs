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

use crate::error::ArchiveResult;
use futures::future::{Future, FutureExt};
use std::pin::Pin;
use std::{collections::VecDeque, marker::PhantomData};
use xtra::prelude::*;
use xtra::{Disconnected, WeakAddress};

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
    /// and is not taken out. WeakAddresses can be used to access
    /// state directly on the actor.
    ///
    /// # None
    /// Returns none if there are no actors left in the pool
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
        M: Message,
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
    R: Send + 'static,
{
    // we create a channel with a capacity of one so that
    // the send does not block the runtime
    let (tx, mut rx) = flume::bounded(1);
    crate::util::spawn(async move {
        match fut.await {
            Ok(v) => {
                if let Err(e) = tx.try_send(v) {
                    match e {
                        flume::TrySendError::Disconnected(_) => {
                            // Receiver might just want to throw out the value (IE `do_send`)
                            // we do nothing.
                        }
                        flume::TrySendError::Full(_) => {
                            log::warn!("Oneshot channel full!"); // this should never happen
                        }
                    }
                }
            }
            Err(_) => {
                log::error!(
                    "One of the pooled db actors has disconnected. could not send message."
                );
                panic!("Actor Disconnected");
            }
        };
        Ok(())
    });
    async move { rx.recv_async().map(|r| r.expect("One shot")).await }.boxed()
}

impl<A: Actor> Actor for ActorPool<A> {}

// We need a concrete struct for this otherwise our handler implementation
// conflicts with xtra's generic implementation for all T
pub struct PoolMessage<M: Message + Send>(pub M);

impl<M> Message for PoolMessage<M>
where
    M: Message + Send,
{
    type Result = Pin<Box<dyn Future<Output = M::Result> + Send + 'static>>;
}

impl<A, M> SyncHandler<PoolMessage<M>> for ActorPool<A>
where
    A: Actor + Send + Clone + Handler<M>,
    M: Message + Send,
{
    fn handle(
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
