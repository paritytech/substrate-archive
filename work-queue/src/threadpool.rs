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

//! Thin wrapepr around `threadpool` to attach a RabbitMQ Consume Connection

use std::{cell::RefCell, sync::Arc};

use lapin::{Channel, Connection, ConnectionProperties};
use async_amqp::LapinAsyncStdExt;
use threadpool::ThreadPool;

use crate::error::*;

thread_local!(static CHANNEL: RefCell<Option<Channel>> = Default::default());

pub struct ThreadPoolMq {
    conn: Arc<lapin::Connection>,
    pool: ThreadPool,
}

impl ThreadPoolMq {
    
    pub fn with_name(name: &str, threads: usize, addr: &str) -> Result<Self, Error> {
        
        let conn = Arc::new(Connection::connect(addr, ConnectionProperties::default().with_async_std()).wait()?);
        Ok(Self { 
            pool: ThreadPool::with_name(name.into(), threads),
            conn
        })
    }

    pub fn execute<F>(&self, job: F)
    where
        F: Send + 'static + FnOnce(&mut Channel),
    {
        let conn = self.conn.clone();
        self.pool.execute(move || {
            init_if_uninit(&conn, job);
        })
    }
}

/// initialize thread-local variables if uninitialized
fn init_if_uninit<F>(conn: &lapin::Connection, job: F) 
where
    F: Send + 'static + FnOnce(&mut Channel) 
{
    CHANNEL.with(|f| {
        // FIXME: not entirely sure if this is safe or the best way to do this. Seems pretty
        // finicky with this Option<>
        let mut chan = f.borrow_mut();
        let chan = chan.get_or_insert_with(|| conn.create_channel().wait().ok().expect("Channel Creation should be flawless"));
        job(chan)
    })
}


#[cfg(test)]
mod tests {
    use super::*; 



}
