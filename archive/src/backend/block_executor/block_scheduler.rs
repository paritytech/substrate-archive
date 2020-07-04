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
use codec::{Decode, Encode};
use hashbrown::HashSet;
use sc_client_api::backend;
use sp_api::{ApiExt, ApiRef, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

/// Encoded version of the data coming in
/// the and identifier is kept decoded so that it may be sorted
/// this is more memory efficient than keeping the rust representation in memory
#[derive(Clone)]
struct EncodedIn<I: PriorityIdent> {
    enc: Vec<u8>,
    id: I::Ident,
}

impl<I: PriorityIdent> Ord for EncodedIn<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<I: PriorityIdent> PartialEq for EncodedIn<I> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<I: PriorityIdent> PartialOrd for EncodedIn<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: PriorityIdent> Eq for EncodedIn<I> {}

impl<I: Encode + PriorityIdent> From<I> for EncodedIn<I> {
    fn from(d: I) -> EncodedIn<I> {
        let id = d.identifier();
        EncodedIn {
            enc: d.encode(),
            id,
        }
    }
}

pub struct BlockScheduler<I, O, T>
where
    I: Clone + Send + Sync + Encode + Decode + PriorityIdent,
    O: Send + Sync + Debug,
    T: ThreadPool<In = I, Out = O>,
{
    /// sorted prioritized queue of blocks
    queue: BinaryHeap<EncodedIn<I>>, // EncodedBlockSpec
    /// A HashSet of the data to be inserted. Used for checking against duplicates
    dups: HashSet<Vec<u8>>,
    sender: flume::Sender<O>, // BlockChanges<B>
    exec: T,
    // internal sender/receivers for gauging how much work
    // the threadpool has finished
    tx: flume::Sender<O>,
    rx: flume::Receiver<O>,
    added: usize,
    finished: usize,
    max_size: usize,
}

impl<I, O, T> BlockScheduler<I, O, T>
where
    I: Clone + Send + Sync + Encode + Decode + PriorityIdent + Debug,
    O: Send + Sync + Debug,
    T: ThreadPool<In = I, Out = O>,
{
    pub fn new(exec: T, sender: flume::Sender<O>, max_size: usize) -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            queue: BinaryHeap::new(),
            dups: HashSet::new(),
            sender,
            exec,
            tx,
            rx,
            added: 0,
            finished: 0,
            max_size,
        }
    }

    // BlockData<B>
    pub fn add_data(&mut self, data: Vec<I>) {
        // filter for duplicates
        let data = data
            .into_iter()
            .map(|d| EncodedIn::from(d))
            .filter(|d| !self.dups.contains(&d.enc))
            .collect::<Vec<_>>();
        self.dups.extend(data.iter().map(|d| d.enc.clone()));
        self.queue.extend(data.into_iter());
    }

    pub fn check_work(&mut self) -> ArchiveResult<()> {
        log::debug!("Queue Length: {}", self.queue.len());
        log::debug!(
            "Queue Size: {}",
            std::mem::size_of::<EncodedIn<I>>() * self.queue.len()
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

        let mut temp_fin = 0;
        self.rx.drain().for_each(|c| {
            temp_fin += 1;
            self.sender.send(c).unwrap()
        });
        self.finished += temp_fin;
        Ok(())
    }

    fn add_work(&mut self, to_add: usize) -> ArchiveResult<()> {
        let mut sorted = BinaryHeap::new();
        std::mem::swap(&mut self.queue, &mut sorted);

        let mut sorted = sorted.into_sorted_vec();
        let to_insert = if sorted.len() > to_add {
            sorted
                .drain(0..to_add)
                .map(|b| Decode::decode(&mut b.enc.as_slice()).map_err(ArchiveError::from))
                .collect::<ArchiveResult<Vec<I>>>()?
        } else {
            sorted
                .drain(0..)
                .map(|b| Decode::decode(&mut b.enc.as_slice()).map_err(ArchiveError::from))
                .collect::<ArchiveResult<Vec<I>>>()?
        };
        self.queue.extend(sorted.into_iter());

        self.added += self.exec.add_task(to_insert, self.tx.clone())?;
        Ok(())
    }
}
