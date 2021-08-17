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


use antidote::{Mutex, MutexGuard};
use sa_work_queue::{Builder, Runner};
use once_cell::sync::Lazy;
use std::ops::{Deref, DerefMut};
use std::time::Duration;
// Since these tests deal with behavior concerning multiple connections
// running concurrently, they have to run outside of a transaction.
// Therefore we can't run more than one at a time.
//
// Rather than forcing the whole suite to be run with `--test-threads 1`,
// we just lock these tests instead.
static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub struct TestGuard<'a, Env: 'static> {
    runner: Runner<Env>,
    _lock: MutexGuard<'a, ()>,
}

impl<'a, Env> TestGuard<'a, Env> {
    pub fn builder(env: Env) -> GuardBuilder<Env> {
        let builder = Runner::builder(env, test_common::AMQP_URL).queue_name(test_common::TASK_QUEUE).prefetch(1);
        GuardBuilder { builder }
    }

    pub fn runner(env: Env) -> Self {
        Self::builder(env).num_threads(4).build()
    }
}

impl<'a> TestGuard<'a, ()> {
    pub fn dummy_runner() -> Self {
        Self::builder(()).num_threads(1).build()
    }
}

pub struct GuardBuilder<Env: 'static> {
    builder: Builder<Env>,
}

impl<Env> GuardBuilder<Env> {
    pub fn register_job<T: sa_work_queue::Job + 'static + Send>(mut self) -> Self {
        self.builder = self.builder.register_job::<T>();
        self
    }

    pub fn num_threads(mut self, threads: usize) -> Self {
        self.builder = self.builder.num_threads(threads);
        self
    }

    /// Set a timeout in seconds.
    /// This is the maximum amount of time we will wait until classifying a task as a failure and updating the retry counter.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.timeout(timeout);
        self
    }

    pub fn build<'a>(self) -> TestGuard<'a, Env> {
        TestGuard {
            _lock: TEST_MUTEX.lock(),
            runner: self.builder.build().unwrap(),
        }
    }
}

impl<'a, Env> Deref for TestGuard<'a, Env> {
    type Target = Runner<Env>;

    fn deref(&self) -> &Self::Target {
        &self.runner
    }
}

impl<'a, Env> DerefMut for TestGuard<'a, Env> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runner
    }
}

// makes sure all Pg connections are closed and database is empty before running any other tests
impl<'a, Env: 'static> Drop for TestGuard<'a, Env> {
    fn drop(&mut self) {
        let handle = self.runner.handle();
        handle.channel().queue_delete(handle.name().into(), Default::default()).wait().unwrap();
    }
}
