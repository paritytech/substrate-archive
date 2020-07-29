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
// use self::block_fetcher::ThreadedBlockFetcher;
use self::block_scheduler::BlockScheduler;
use crate::backend::{ApiAccess, BlockChanges, ReadOnlyBackend as Backend};
use crate::{actors::ActorContext, error::ArchiveResult, types::Block};
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
    sender: async_channel::Sender<BlockData<B>>,
    _handle: jod_thread::JoinHandle<ArchiveResult<()>>,
    pair: (
        async_channel::Sender<BlockChanges<B>>,
        Option<async_channel::Receiver<BlockChanges<B>>>,
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
        let (tx, rx) = async_channel::unbounded();
        let (sender, receiver) = async_channel::unbounded();
        let res_sender = sender.clone();
        let handle = jod_thread::spawn(move || -> ArchiveResult<()> {
            let pool = BlockExecPool::<B, R, A>::new(threads, client, backend)?;
            let mut pool = BlockScheduler::new("exec", pool, 256, Ordering::Ascending);
            'sched: loop {
                thread::sleep(Duration::from_millis(50));
                for _ in 0..rx.len() {
                    match rx.try_recv() {
                        Ok(v) => match v {
                            BlockData::Batch(v) => pool.add_data(v),
                            BlockData::Single(v) => pool.add_data_single(v),
                        },
                        Err(e) => match e {
                            async_channel::TryRecvError::Closed => break 'sched,
                            _ => (),
                        },
                    }
                }
                for w in pool.check_work()?.into_iter() {
                    match res_sender.try_send(w) {
                        Ok(_) => (),
                        Err(e) => match e {
                            async_channel::TrySendError::Closed(_) => break 'sched,
                            _ => (), // Channels are unbounded so should never be full
                        },
                    }
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
                if let Err(_) = tx.send(m).await {
                    break;
                }
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
    pub fn sender(&self) -> async_channel::Sender<BlockData<B>> {
        self.sender.clone()
    }
}
