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
use futures::future::Future;
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
    pub fn forward<M>(&mut self, msg: M)
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

// TODO: Could have a message which pulls an actor out of the queue completely, and rejoins it to
// the queue when that Address is dropped
// This message would have to return a Box<dyn AddressExt>, however, since it's implementation
// would require something like a struct `RejoinOnDrop` that sends the address back to the pool to
// Re-add it to the queue. RejoinOnDrop would implement AddressExt.
// this avoids having to call methods like `add` when working directly with a Strong Addres
// But it's only really useful if a strong address is needed (IE when we don't want the actor to
// be dropped if there is unfinished work to do).
/// Gets a weak address to one of the actors in the pool
pub struct PoolConnection<A: Actor + Send>(PhantomData<A>);

// if we don't implement this manually, it requires that the actor implement
// Default
impl<A: Actor + Send> Default for PoolConnection<A> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<A: Actor + Send> Message for PoolConnection<A> {
    type Result = Option<WeakAddress<A>>;
}

impl<A: Actor + Send + Clone> SyncHandler<PoolConnection<A>> for ActorPool<A> {
    fn handle(&mut self, _: PoolConnection<A>, _: &mut Context<Self>) -> Option<WeakAddress<A>> {
        self.pull_weak()
    }
}

// We need a concrete struct for this otherwise our handler implementation
// conflicts with xtra's generic implementation for all T
pub struct PoolMessage<M: Message + Send>(pub M);

impl<M> Message for PoolMessage<M>
where
    M: Message + Send,
{
    type Result = ();
}

impl<A, M> SyncHandler<PoolMessage<M>> for ActorPool<A>
where
    A: Actor + Send + Clone + Handler<M>,
    M: Message<Result = ArchiveResult<()>> + Send,
{
    fn handle(&mut self, msg: PoolMessage<M>, _: &mut Context<Self>) {
        self.forward(msg.0);
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
