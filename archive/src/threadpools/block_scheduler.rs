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

//! Schedules blocks based upon a pre-defined priority
//! this could be EX runtime-version or block_number
//! This allows us to use the runtime-code cache to it's fullest potential
//! and reduces reads on the rocksdb backend
//! without rewriting `NativeExecutor` in substrate
//! This is most useful when there are many blocks in the queue (IE, syncing with a chain
//! that is already fully-synced). Maintains a buffer of sorted blocks to execute, and sorts blocks
//! that are being streamed

use crate::{
    error::{ArchiveResult, Error},
    types::*,
    util::make_hash,
};
use codec::{Decode, Encode};
use hashbrown::HashSet;
use std::{collections::BinaryHeap, fmt::Debug, hash::Hash};

// TODO Get rid of the HashSet redundant checking for duplicates if possible.
// TODO Just store generic strut instead of the encoded version of the struct.
// I doubt that it is much more memory efficient to temporarily store encoded version

/// Encoded version of the data coming in
/// the and identifier is kept decoded so that it may be sorted
/// this could be more memory efficient than keeping the rust representation in memory
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
/// The ordering in which items are given to the threadpool
pub enum Ordering {
    Ascending,
    Descending,
}

pub struct BlockScheduler<I, O, T>
where
    I: Clone + Send + Sync + Encode + Decode + PriorityIdent,
    O: Send + Sync + Debug,
    T: ThreadPool<In = I, Out = O>,
{
    name: String,
    /// sorted prioritized queue of blocks
    queue: BinaryHeap<EncodedIn<I>>,
    /// A HashSet of the data to be inserted (hashed before inserted into the HashSet).
    /// Used to check for duplicates
    dups: HashSet<u64>,
    /// the threadpool
    exec: T,
    /// internal sender for gauging how much work
    /// the threadpool has finished
    tx: async_channel::Sender<O>,
    /// internal receiver for gauging how much work
    /// the threadpool has finished
    rx: async_channel::Receiver<O>,
    /// how many total items have been added to the threadpool
    added: usize,
    /// how many tasks has the threadpool already finished
    finished: usize,
    /// the maximum tasks we should have queued in the threadpool at any one time
    max_size: usize,
    /// The order in which we should schedule based on the priority identifier
    ordering: Ordering,
}

impl<I, O, T> BlockScheduler<I, O, T>
where
    I: Clone + Send + Sync + Encode + Decode + PriorityIdent + Debug,
    O: Send + Sync + Debug + Clone,
    T: ThreadPool<In = I, Out = O>,
{
    pub fn new(name: &str, exec: T, max_size: usize, ord: Ordering) -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self {
            name: name.to_string(),
            queue: BinaryHeap::new(),
            dups: HashSet::new(),
            exec,
            tx,
            rx,
            added: 0,
            finished: 0,
            max_size,
            ordering: ord,
        }
    }

    pub fn add_data(&mut self, data: Vec<I>)
    where
        I::Ident: Debug,
    {
        let data = data
            .into_iter()
            .map(EncodedIn::from)
            .filter(|d| !self.dups.contains(&make_hash(&d.enc)))
            .collect::<Vec<_>>();
        self.dups
            .extend(data.iter().map(|d| make_hash(d.enc.as_slice())));
        self.queue.extend(data.into_iter());
    }

    pub fn add_data_single(&mut self, data: I) {
        let data = EncodedIn::from(data);
        let hash = make_hash(&data.enc);
        if !self.dups.contains(&hash) {
            self.dups.insert(hash);
            self.queue.push(data)
        }
    }

    pub fn check_work(&mut self) -> ArchiveResult<Vec<O>> {
        // we try to maintain a MAX queue of max_size tasks at a time in the threadpool
        let delta = self.added - self.finished;
        if self.finished == 0 && self.added == 0 {
            self.add_work(self.max_size)?;
        } else if delta < self.max_size && delta > 0 {
            self.add_work(self.max_size - delta)?;
        } else if delta == 0 && self.queue.len() > self.max_size {
            self.add_work(self.max_size)?;
        } else if delta == 0 && self.queue.len() <= self.max_size {
            self.add_work(self.queue.len())?;
        } else {
            log::debug!(
                "sched-{}: Queue Length: {}, Delta: {}, added: {}, finished: {}",
                self.name,
                self.queue.len(),
                delta,
                self.added,
                self.finished
            );
        }

        let mut out: Vec<O> = Vec::new();
        for _ in 0..self.rx.len() {
            match self.rx.try_recv() {
                Ok(v) => out.push(v),
                Err(_) => {
                    return Err(Error::Disconnected);
                }
            }
        }
        self.finished += out.len();
        Ok(out)
    }

    fn add_work(&mut self, to_add: usize) -> ArchiveResult<()> {
        let mut sorted = BinaryHeap::new();
        std::mem::swap(&mut self.queue, &mut sorted);

        let mut sorted = {
            let mut s = sorted.into_sorted_vec();
            match self.ordering {
                Ordering::Ascending => (),
                Ordering::Descending => s.reverse(),
            }
            s
        };
        let to_insert = if sorted.len() > to_add {
            sorted
                .drain(0..to_add)
                .map(|b| Decode::decode(&mut b.enc.as_slice()).map_err(Error::from))
                .collect::<ArchiveResult<Vec<I>>>()?
        } else {
            sorted
                .drain(0..)
                .map(|b| Decode::decode(&mut b.enc.as_slice()).map_err(Error::from))
                .collect::<ArchiveResult<Vec<I>>>()?
        };
        if matches!(self.ordering, Ordering::Descending) {
            sorted.reverse();
        }
        self.queue.extend(sorted.into_iter());
        self.added += self.exec.add_task(to_insert, self.tx.clone())?;
        Ok(())
    }
}
