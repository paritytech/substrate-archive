// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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
// along with substrate-archive. If not, see <http://www.gnu.org/licenses/>.


use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::{Arc, Barrier as StdBarrier, BarrierWaitResult};

#[derive(Clone)]
pub struct Barrier {
    inner: Arc<StdBarrier>,
}

impl Barrier {
    pub fn new(n: usize) -> Self {
        Self {
            inner: Arc::new(StdBarrier::new(n)),
        }
    }

    pub fn wait(&self) -> BarrierWaitResult {
        self.inner.wait()
    }
}

impl UnwindSafe for Barrier {}
impl RefUnwindSafe for Barrier {}
