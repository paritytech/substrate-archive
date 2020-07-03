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

//! Tries to Schedule continguously runtime-versioned blocks onto a threadpool
//! This allows us to use the runtime-code cache to it's fullest potential
//! and reduces reads on the rocksdb backend
//! without rewriting `NativeExecutor` in substrate
//! This is most useful when there are many blocks in the queue (IE, syncing with a chain
//! that is already fully-synced). Maintains a buffer of sorted blocks to execute, and sorts blocks
//! that are being streamed

use super::{BlockChanges, BlockData, BlockSpec, ThreadedBlockExecutor};
use crate::{
    backend::{ApiAccess, ReadOnlyBackend as Backend},
    error::{ArchiveResult, Error as ArchiveError},
    types::*,
};
use async_channel::Sender;
use codec::{Decode, Encode};
use sc_client_api::backend;
use sp_api::{ApiExt, ApiRef, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::collections::BinaryHeap;
use std::sync::Arc;

/// Encoded version of BlockSpec
/// the spec version is not encoded so that it may be sorted
/// this is more memory efficient than keeping the rust representation in memory
#[derive(Clone, Eq)]
struct EncodedBlockSpec {
    block: Vec<u8>,
    spec: u32,
}

impl Ord for EncodedBlockSpec {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.spec.cmp(&other.spec)
    }
}

impl PartialEq for EncodedBlockSpec {
    fn eq(&self, other: &Self) -> bool {
        self.spec == other.spec
    }
}

impl PartialOrd for EncodedBlockSpec {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<B: BlockT> From<EncodedBlockSpec> for BlockSpec<B> {
    fn from(enc: EncodedBlockSpec) -> BlockSpec<B> {
        let block = Decode::decode(&mut enc.block.as_slice()).unwrap();
        BlockSpec {
            block,
            spec: enc.spec,
        }
    }
}

impl<B: BlockT> From<BlockSpec<B>> for EncodedBlockSpec {
    fn from(bspec: BlockSpec<B>) -> EncodedBlockSpec {
        let block = bspec.block.encode();
        EncodedBlockSpec {
            block,
            spec: bspec.spec,
        }
    }
}

pub struct BlockScheduler<B: BlockT, RA, Api> {
    /// sorted prioritized queue of blocks
    queue: BinaryHeap<EncodedBlockSpec>,
    backend: Arc<Backend<B>>,
    client: Arc<Api>,
    sender: Sender<BlockChanges<B>>,
    exec: ThreadedBlockExecutor<B, RA, Api>,

    // internal sender/receivers for gauging how much work
    // the threadpool has finished
    tx: crossbeam::channel::Sender<BlockChanges<B>>,
    rx: crossbeam::channel::Receiver<BlockChanges<B>>,
    added: usize,
    finished: usize,
}

impl<B, RA, Api> BlockScheduler<B, RA, Api>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
    RA: ConstructRuntimeApi<B, Api> + Send + 'static,
    RA::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = backend::StateBackendFor<Backend<B>, B>>,
    Api: ApiAccess<B, Backend<B>, RA> + 'static,
{
    pub fn new(
        exec: ThreadedBlockExecutor<B, RA, Api>,
        backend: Arc<Backend<B>>,
        client: Arc<Api>,
        sender: Sender<BlockChanges<B>>,
    ) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded();
        Self {
            queue: BinaryHeap::new(),
            backend,
            client,
            sender,
            exec,
            tx,
            rx,
            added: 0,
            finished: 0,
        }
    }

    pub fn add_data(&mut self, data: BlockData<B>) {
        match data {
            BlockData::Batch(v) => self.queue.extend(v.into_iter().map(|v| v.into())),
            BlockData::Single(v) => self.queue.push(v.into()),
            Stop => unimplemented!(),
        }
    }

    pub fn check_work(&mut self) -> ArchiveResult<()> {
        log::debug!("Queue Length: {}", self.queue.len());
        log::debug!(
            "Queue Size: {}",
            std::mem::size_of::<EncodedBlockSpec>() * self.queue.len()
        );
        // we try to maintain a MAX queue of 256 tasks at a time in the threadpool
        let delta = self.added - self.finished;
        if delta < 256 && delta > 0 {
            self.add_work(delta)?;
        } else if self.finished == 0 && self.added == 0 && self.queue.len() > 256 {
            self.add_work(256)?;
        } else if delta == 0 && self.queue.len() > 256 {
            self.add_work(256)?;
        }
        log::debug!("AFTER finished: {}, added: {}", self.finished, self.added);
        self.finished += self.rx.len();
        self.rx
            .try_iter()
            .for_each(|c| self.sender.try_send(c).unwrap());
        Ok(())
    }

    fn add_work(&mut self, to_add: usize) -> ArchiveResult<()> {
        let mut sorted = BinaryHeap::new();
        std::mem::swap(&mut self.queue, &mut sorted);

        let mut sorted = sorted.into_sorted_vec().into_iter().collect::<Vec<_>>();
        let to_insert = if sorted.len() > to_add {
            sorted.drain(0..to_add).map(|b| b.block).collect::<Vec<_>>()
        } else {
            sorted.drain(0..).map(|b| b.block).collect::<Vec<_>>()
        };
        self.queue.extend(sorted.into_iter());

        self.added += self.exec.add_vec_task(
            to_insert,
            self.client.clone(),
            self.backend.clone(),
            self.tx.clone(),
        )?;
        Ok(())
    }
}
