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

//! Wrapper around `threadpool` with an attached RabbitMQ Consume Connection.
//! Each thread in the pool gets its own RabbitMq Consumer, which is passed to the caller.
//! Each instance of a threadpool shares one RabbitMq connection amongst all of its threads.

use std::{cell::RefCell, sync::Arc};

use lapin::{
    Channel, Connection, Consumer, ConnectionProperties, 
    options::{BasicConsumeOptions, BasicAckOptions, BasicNackOptions}, 
    types::FieldTable, message::Delivery,
};
use channel::{Sender, Receiver};
use async_amqp::LapinAsyncStdExt;
use async_std::task;
use futures::{Stream, StreamExt};
use threadpool::ThreadPool;

use crate::{error::*, db::BackgroundJob, runner::Event};

thread_local!(static CONSUMER: RefCell<Option<Consumer>> = Default::default());

pub struct ThreadPoolMq {
    conn: Arc<lapin::Connection>,
    pool: ThreadPool,
    tx: Sender<Event>,
    rx: Receiver<Event>
}

impl ThreadPoolMq {
    
    pub fn with_name(name: &str, threads: usize, addr: &str) -> Result<Self, Error> {
        let conn = Arc::new(Connection::connect(addr, ConnectionProperties::default().with_async_std()).wait()?);
        let pool = ThreadPool::with_name(name.into(), threads);
        let (tx, rx) = channel::bounded(pool.max_count());
        Ok(Self { pool, conn, tx, rx })
    }
    
    /// Execute a job on this threadpool.
    /// Automatically advances RabbitMq queue and feeds 
    /// the payload in to the predicate `F`.
    pub fn execute<F>(&self, job: F)
    where
        F: Send + 'static + FnOnce(BackgroundJob) -> Result<(), PerformError>,
    {
        let conn = self.conn.clone();
        let tx = self.tx.clone();
        self.pool.execute(move || {
            run_job(&conn, tx, job)
        })
    }

    pub fn max_count(&self) -> usize {
        self.pool.max_count()
    }

    pub fn active_count(&self) -> usize {
        self.pool.active_count()     
    }
    
    // TODO: could wrap this so we're not exposing underlying details and just returning a raw
    // receiver
    /// get the receiving end of events sent from the threadpool 
    pub fn events(&self) -> &Receiver<Event> {
        &self.rx
    }
}

/// initialize thread-local variables if uninitialized
fn run_job<F>(conn: &lapin::Connection, tx: Sender<Event>, job: F) 
where
    F: Send + 'static + FnOnce(BackgroundJob) -> Result<(), PerformError>
{
    CONSUMER.with(|c| {
        // FIXME: not entirely sure if this is safe or the best way to do this. Seems pretty
        // finicky with this Option<>
        let mut consumer = c.borrow_mut();
        let mut consumer = consumer.get_or_insert_with(|| {
            let chan = conn.create_channel().wait().ok().expect("Channel Creation should be flawless");
            chan 
                .basic_consume(crate::TASK_QUEUE, "", BasicConsumeOptions::default(), FieldTable::default())
                .wait()
                .ok()
                .expect("prototyping")
        });
        
        if let Some((data, delivery)) = next_job(tx, &mut consumer) {
             match job(data) {
                Ok(_) => {
                    tracing::trace!("Job Success");
                    task::block_on(delivery.acker.ack(BasicAckOptions::default()));
                },
                Err(e) => {
                    task::block_on(delivery.acker.nack(BasicNackOptions {
                        requeue: true,
                        ..Default::default()
                    }));
                    tracing::error!("Job failed to run {}", e);
                    eprintln!("Job failed to run: {}", e);
                }
            }
        }
    })
}


fn next_job(tx: Sender<Event>, consumer: &mut Consumer) -> Option<(BackgroundJob, Delivery)> {
    match get_next_job(consumer) {
        Ok(Some(d)) => { 
            tx.send(Event::Working);
            Some(d)
        },
        Ok(None) => {
            let _ = tx.send(Event::NoJobAvailable);
            return None
        },
        Err(e) => {
            let _ = tx.send(Event::ErrorLoadingJob(e));
            return None
        }
    }
}

fn get_next_job(consumer: &mut Consumer) -> Result<Option<(BackgroundJob, Delivery)>, FetchError> {
    let delivery = task::block_on(consumer.next()).transpose()?.map(|(_, d)| d);
    let data: Option<BackgroundJob> = delivery.as_ref().map(|d| serde_json::from_slice(d.data.as_slice())).transpose()?;
    Ok(data.zip(delivery))
}


#[cfg(test)]
mod tests {
    use super::*; 
}
