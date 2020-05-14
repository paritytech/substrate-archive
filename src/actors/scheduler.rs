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

use bastion::prelude::*;

pub enum Algorithm {
    RoundRobin,
}

pub struct Scheduler {
    last_executed: usize,
    alg: Algorithm,
}

impl Scheduler {
    pub fn new(alg: Algorithm) -> Self {
        Self {
            last_executed: 0,
            alg,
        }
    }

    pub fn next<T>(
        &mut self,
        ctx: &BastionContext,
        workers: &ChildrenRef,
        data: T,
    ) -> Result<Answer, T>
    where
        T: Send + Sync + std::fmt::Debug + 'static,
    {
        match self.alg {
            Algorithm::RoundRobin => {
                self.last_executed += 1;
                let next_executed = self.last_executed % workers.elems().len();
                ctx.ask(&workers.elems()[next_executed].addr(), data)
            }
        }
    }
}
