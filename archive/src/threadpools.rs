// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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

pub use self::block_exec_pool::BlockData;
use self::block_exec_pool::BlockExecPool;
use self::block_fetcher::ThreadedBlockFetcher;
use self::block_scheduler::BlockScheduler;
use crate::backend::{ApiAccess, BlockChanges, ReadOnlyBackend as Backend};
use crate::{
    actors::{ActorContext, Aggregator},
    error::ArchiveResult,
    types::Block,
};
use futures::{Stream, StreamExt};
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header as _, NumberFor},
};
use std::{marker::PhantomData, sync::Arc, thread, time::Duration};
use xtra::prelude::*;

mod block_exec_pool;
mod block_fetcher;
mod block_scheduler;

// TODO: Can abstract these two structs into just something that implements a trait
// this follows a similar API to xtra's Actor/Address api (attach_stream)
// maybe we could create an extension trait that is like Actix's Threadpooled Actors, but for xtra?
// that is essentially what this is trying to be.

/// A threadpool that gets blocks and their runtime versions from the rocksdb backend
pub struct BlockFetcher<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    sender: flume::Sender<u32>,
    pair: (flume::Sender<Block<B>>, flume::Receiver<Block<B>>),
    handle: ThreadJoinHandle,
}

impl<B> BlockFetcher<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    pub fn new(ctx: ActorContext<B>, threads: Option<usize>) -> ArchiveResult<Self> {
        let (tx, rx) = flume::unbounded();
        let (sender, receiver) = flume::unbounded();
        let handle = thread::spawn(move || -> ArchiveResult<()> {
            let pool = ThreadedBlockFetcher::new(ctx, threads)?;
            let pool = BlockScheduler::new(pool, 1000);
            loop {
                thread::sleep(Duration::from_millis(50));
                let msgs = rx.drain().collect::<Vec<u32>>();
                pool.add_data(msgs);
                for w in pool.check_work()?.into_iter() {
                    sender.send(w)?;
                }
            }
            Ok(())
        });

        Ok(Self {
            pair: (sender, receiver),
            sender: tx,
            handle: handle.into(),
        })
    }

    /// attach a stream to this threadpool
    /// Forwards all messages from the stream to the threadpool
    pub fn attach_stream(&self, stream: impl Stream<Item = u32> + Send + Unpin + 'static) {
        let tx = self.sender.clone();
        crate::util::spawn(async move {
            while let Some(m) = stream.next().await {
                tx.send(m)?;
            }
            Ok(())
        });
    }

    /// Convert this Threadpool into a stream of its outputs
    pub fn into_stream(self) -> impl Stream<Item = Block<B>> {
        self.pair.1
    }
}

/// Threadpool that executes blocks
pub struct ThreadedBlockExecutor<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    /// The main sender
    sender: flume::Sender<BlockData<B>>,
    handle: ThreadJoinHandle,
    pair: (
        flume::Sender<BlockChanges<B>>,
        flume::Receiver<BlockChanges<B>>,
    ),
}

impl<B> ThreadedBlockExecutor<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    pub fn new<R, A>(
        client: Arc<A>,
        backend: Arc<Backend<B>>,
        threads: Option<usize>,
    ) -> ArchiveResult<Self>
    where
        R: ConstructRuntimeApi<B, A> + Send + 'static,
        R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
            + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
        A: ApiAccess<B, Backend<B>, R> + 'static,
    {
        let (tx, rx) = flume::unbounded();
        let (sender, receiver) = flume::unbounded();
        let handle = thread::spawn(move || -> ArchiveResult<()> {
            let pool = BlockExecPool::<B, R, A>::new(threads, client, backend)?;
            let pool = BlockScheduler::new(pool, 256);
            loop {
                thread::sleep(Duration::from_millis(50));
                rx.drain().for_each(|v| match v {
                    BlockData::Batch(v) => pool.add_data(v),
                    BlockData::Single(v) => pool.add_data_single(v),
                    BlockData::Stop => unimplemented!(),
                });
                for w in pool.check_work()?.into_iter() {
                    sender.send(w)?;
                }
            }
            Ok(())
        });
        Ok(Self {
            sender: tx,
            pair: (sender, receiver),
            handle: handle.into(),
        })
    }

    /// attach a stream to this threadpool
    /// Forwards all messages from the stream to the threadpool
    pub fn attach_stream(&self, stream: impl Stream<Item = BlockData<B>> + Send + Unpin + 'static) {
        let tx = self.sender.clone();
        crate::util::spawn(async move {
            while let Some(m) = stream.next().await {
                tx.send(m)?;
            }
            Ok(())
        });
    }

    /// Convert this Threadpool into a stream of its outputs
    pub fn into_stream(self) -> impl Stream<Item = BlockChanges<B>> {
        self.pair.1
    }

    /// Get the sender for this threadpool
    pub fn sender(&self) -> flume::Sender<BlockData<B>> {
        self.sender.clone()
    }
}

struct ThreadJoinHandle(thread::JoinHandle<ArchiveResult<()>>);

impl From<thread::JoinHandle<ArchiveResult<()>>> for ThreadJoinHandle {
    fn from(j: thread::JoinHandle<ArchiveResult<()>>) -> ThreadJoinHandle {
        ThreadJoinHandle(j)
    }
}

impl Drop for ThreadJoinHandle {
    fn drop(&mut self) {
        match self.0.join().unwrap() {
            Ok(_) => (),
            Err(e) => log::error!("Thread Exited with Error: {}", e.to_string()),
        }
        /*
        match std::panic::catch_unwind(self.0.join().unwrap()) {
            Ok(_) => (),
            Err(e) => log::error!("{:?}", e),
        }
        */
    }
}
