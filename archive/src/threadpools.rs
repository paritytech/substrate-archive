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
use self::block_scheduler::BlockScheduler;
use crate::backend::{ApiAccess, BlockChanges, ReadOnlyBackend as Backend};
use crate::error::Result;
use block_scheduler::Ordering;
use futures::{Stream, StreamExt};
use sc_client_api::backend;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::{sync::Arc, thread, time::Duration};
mod block_exec_pool;
mod block_scheduler;

/// Threadpool that executes blocks
pub struct ThreadedBlockExecutor<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    /// The main sender
    sender: flume::Sender<BlockData<B>>,
    _handle: jod_thread::JoinHandle<Result<()>>,
    pair: (
        flume::Sender<BlockChanges<B>>,
        Option<flume::Receiver<BlockChanges<B>>>,
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
    ) -> Result<Self>
    where
        R: ConstructRuntimeApi<B, A> + Send + 'static,
        R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
            + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
        A: ApiAccess<B, Backend<B>, R> + 'static,
    {
        let (tx, rx) = flume::unbounded();
        let (sender, receiver) = flume::unbounded();
        let res_sender = sender.clone();
        let handle = jod_thread::spawn(move || -> Result<()> {
            let pool = BlockExecPool::<B, R, A>::new(threads, client, backend)?;
            let mut pool = BlockScheduler::new("exec", pool, 256, Ordering::Ascending);
            'sched: loop {
                thread::sleep(Duration::from_millis(50));
                // ideally, there should be a way to check if senders
                // have dropped: https://github.com/zesterer/flume/issues/32
                // instead we just recv one message and see if it's disconnected
                // before draining the queue
                match rx.try_recv() {
                    Ok(v) => match v {
                        BlockData::Batch(v) => pool.add_data(v),
                        BlockData::Single(v) => pool.add_data_single(v),
                    },
                    Err(e) => match e {
                        flume::TryRecvError::Disconnected => break 'sched,
                        _ => (),
                    },
                }
                rx.drain().for_each(|v| match v {
                    BlockData::Batch(v) => pool.add_data(v),
                    BlockData::Single(v) => pool.add_data_single(v),
                });
                for w in pool.check_work()?.into_iter() {
                    res_sender.send(w)?;
                }
            }
            Ok(())
        });

        Ok(Self {
            sender: tx,
            pair: (sender, Some(receiver)),
            _handle: handle,
        })
    }

    /// attach a stream to this threadpool
    /// Forwards all messages from the stream to the threadpool
    #[allow(unused)]
    pub fn attach_stream(
        &self,
        mut stream: impl Stream<Item = BlockData<B>> + Send + Unpin + 'static,
    ) {
        let tx = self.sender.clone();
        crate::util::spawn(async move {
            while let Some(m) = stream.next().await {
                tx.send(m)?;
            }
            Ok(())
        });
    }

    /// Convert this Threadpool into a stream of its outputs
    /// # Panics
    /// panics if the stream has already been taken
    pub fn get_stream(&mut self) -> impl Stream<Item = BlockChanges<B>> {
        self.pair.1.take().unwrap()
    }

    /// Get the sender for this threadpool
    pub fn sender(&self) -> flume::Sender<BlockData<B>> {
        self.sender.clone()
    }
}
