// Copyright 2017-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by // the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

//! Actor that takes context and workers and schedules them according to a scheduling algorithm
//! currently only supports 'RoundRobin'

// TODO: Maybe make an actor (supervisor?) that schedules other actors
// otherwise there could be conflicts where one actor starves another because its waiting on some work to finish,
// whereas there are other redundant workers sitting idly
use bastion::prelude::*;
use std::collections::HashMap;
use crate::error::Error as ArchiveError;

pub enum Algorithm {
    RoundRobin,
}

pub struct Scheduler<'a> {
    last_executed: usize,
    alg: Algorithm,
    ctx: &'a BastionContext,
    workers: HashMap<&'a str, &'a ChildrenRef>,
}

impl<'a> Scheduler<'a> {
    pub fn new(alg: Algorithm, ctx: &'a BastionContext) -> Self {
        Self {
            last_executed: 0,
            workers: HashMap::new(),
            alg,
            ctx,
        }
    }

    pub fn add_worker(&mut self, name: &'a str, workers: &'a ChildrenRef) {
        self.workers.insert(name, workers);
    }

    pub fn ask_next<T>(&mut self, name: &str, data: T) -> Result<Answer, ArchiveError>
    where
        T: Send + Sync + std::fmt::Debug + 'static,
    {
        match self.alg {
            Algorithm::RoundRobin => {
                if let Some(w) = self.worker(name) {
                    self.last_executed += 1;
                    let next_executed = self.last_executed % w.elems().len();
                    self.ctx.ask(&w.elems()[next_executed].addr(), data)
                        .map_err(|d| data_to_err(d, "`ask` failed with"))
                } else {
                    Err(data_to_err(data, format!("could not find worker {} to tell", name).as_str()))
                }
            }
        }
    }

    pub fn tell_next<T>(&mut self, name: &str, data: T) -> Result<(), ArchiveError>
    where
        T: Send + Sync + std::fmt::Debug + 'static,
    {
        match self.alg {
            Algorithm::RoundRobin => {
                if let Some(w) = self.worker(name) {
                    self.last_executed += 1;
                    let next_executed = self.last_executed % w.elems().len();
                    self.ctx.tell(&w.elems()[next_executed].addr(), data)
                        .map_err(|d| data_to_err(d, "`tell` failed with "))
                } else {
                    Err(data_to_err(data, format!("could not find worker {} to tell", name).as_str()))
                }
            }
        }
    }

    pub fn worker<'b>(&'b self, name: &str) -> Option<&'a ChildrenRef> {
        self.workers.get(name).map(|w| *w)
    }

    pub fn context(&'a self) -> &'a BastionContext {
        self.ctx
    }
}

fn data_to_err<T>(data: T, msg: &str) -> ArchiveError
where
    T: Send + Sync + std::fmt::Debug + 'static
{
    ArchiveError::from(format!("{}: {:?}", msg, data).as_str())
}
