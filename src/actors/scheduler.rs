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

//! Actor that takes context and workers and schedules them according to a scheduling algorithm
//! currently only supports 'RoundRobin'

// TODO: Maybe make an actor (supervisor?) that schedules other actors
// otherwise there could be conflicts where one actor starves another because its waiting on some work to finish,
// whereas there are other redundant workers sitting idly
use bastion::prelude::*;

pub enum Algorithm {
    RoundRobin,
}

pub struct Scheduler<'a> {
    last_executed: usize,
    alg: Algorithm,
    ctx: &'a BastionContext,
    workers: &'a ChildrenRef,
}

impl<'a> Scheduler<'a> {
    pub fn new(alg: Algorithm, ctx: &'a BastionContext, workers: &'a ChildrenRef) -> Self {
        Self {
            last_executed: 0,
            alg,
            ctx,
            workers,
        }
    }

    pub fn ask_next<T>(&mut self, data: T) -> Result<Answer, T>
    where
        T: Send + Sync + std::fmt::Debug + 'static,
    {
        match self.alg {
            Algorithm::RoundRobin => {
                self.last_executed += 1;
                let next_executed = self.last_executed % self.workers.elems().len();
                self.ctx
                    .ask(&self.workers.elems()[next_executed].addr(), data)
            }
        }
    }

    pub fn tell_next<T>(&mut self, data: T) -> Result<(), T>
    where
        T: Send + Sync + std::fmt::Debug + 'static,
    {
        match self.alg {
            Algorithm::RoundRobin => {
                self.last_executed += 1;
                let next_executed = self.last_executed % self.workers.elems().len();
                self.ctx
                    .tell(&self.workers.elems()[next_executed].addr(), data)
            }
        }
    }
}
