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

use crate::{
    error::{ArchiveResult, Error as ArchiveError},
    types::*,
};
use codec::{Decode, Encode};
use hashbrown::HashSet;
use std::{collections::BinaryHeap, fmt::Debug};

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
    queue: BinaryHeap<EncodedIn<I>>,
    /// A HashSet of the data to be inserted. Used for checking against duplicates
    dups: HashSet<Vec<u8>>,
    /// the threadpool
    exec: T,
    /// internal sender for gauging how much work
    /// the threadpool has finished
    tx: flume::Sender<O>,
    /// internal receiver for gauging how much work
    /// the threadpool has finished
    rx: flume::Receiver<O>,
    /// how many total items have been added to the threadpool
    added: usize,
    /// how many tasks has the threadpool already finished
    finished: usize,
    /// the maximum tasks we should have queued in the threadpool at any one time
    max_size: usize,
}

impl<I, O, T> BlockScheduler<I, O, T>
where
    I: Clone + Send + Sync + Encode + Decode + PriorityIdent + Debug,
    O: Send + Sync + Debug + Clone,
    T: ThreadPool<In = I, Out = O>,
{
    pub fn new(exec: T, max_size: usize) -> Self {
        let (tx, rx) = flume::unbounded();
        Self {
            queue: BinaryHeap::new(),
            dups: HashSet::new(),
            exec,
            tx,
            rx,
            added: 0,
            finished: 0,
            max_size,
        }
    }

    pub fn add_data(&mut self, data: Vec<I>) {
        // filter for duplicates
        let data = data
            .into_iter()
            .map(EncodedIn::from)
            .filter(|d| !self.dups.contains(&d.enc))
            .collect::<Vec<_>>();
        self.dups.extend(data.iter().map(|d| d.enc.clone()));
        self.queue.extend(data.into_iter());
    }

    pub fn add_data_single(&mut self, data: I) {
        let data = EncodedIn::from(data);
        if !self.dups.contains(&data.enc) {
            self.dups.insert(data.enc.clone());
            self.queue.push(data)
        }
    }

    pub fn check_work(&mut self) -> ArchiveResult<Vec<O>> {
        log::debug!("Queue Length: {}", self.queue.len());
        // we try to maintain a MAX queue of 256 tasks at a time in the threadpool
        let delta = self.added - self.finished;
        if self.finished == 0 && self.added == 0 && self.queue.len() > self.max_size {
            self.add_work(self.max_size)?;
        } else if delta < self.max_size && delta > 0 {
            self.add_work(self.max_size - delta)?;
        } else if delta == 0 && self.queue.len() > self.max_size {
            self.add_work(self.max_size)?;
        }
        log::debug!("AFTER finished: {}, added: {}", self.finished, self.added);

        let out = self.rx.drain().collect::<Vec<O>>();
        self.finished += out.len();
        Ok(out)
    }

    fn add_work(&mut self, to_add: usize) -> ArchiveResult<()> {
        let mut sorted = BinaryHeap::new();
        std::mem::swap(&mut self.queue, &mut sorted);

        let mut sorted = sorted.into_sorted_vec();
        log::debug!(
            "Queue Size: {} MB",
            size_of_encoded(Deno::MB, sorted.as_slice())
        );
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
        log::debug!(
            "Decoded Queue Size: {} KB",
            size_of_decoded(Deno::KB, to_insert.as_slice())
        );
        self.added += self.exec.add_task(to_insert, self.tx.clone())?;
        Ok(())
    }
}

/// Denomination of bytes/megabytes/kilobytes
enum Deno {
    #[allow(unused)]
    Bytes,
    KB,
    MB,
}

fn size_of_encoded<I: PriorityIdent>(deno: Deno, items: &[EncodedIn<I>]) -> usize {
    let byte_size = || {
        let mut total_size = 0;
        for i in items.iter() {
            let vec_size_bytes = i.enc.len();
            let ident_size = std::mem::size_of::<I::Ident>();
            total_size += vec_size_bytes + ident_size;
        }
        total_size
    };
    match deno {
        Deno::Bytes => byte_size(),
        Deno::KB => byte_size() / 1024,
        Deno::MB => byte_size() / 1024 / 1024,
    }
}

fn size_of_decoded<I: PriorityIdent>(deno: Deno, items: &[I]) -> usize {
    let byte_size = || {
        let mut total_size = 0;
        for _ in items.iter() {
            let item_size_bytes = std::mem::size_of::<I>();
            total_size += item_size_bytes;
        }
        total_size
    };
    match deno {
        Deno::Bytes => byte_size(),
        Deno::KB => byte_size() / 1024,
        Deno::MB => byte_size() / 1024 / 1024,
    }
}
