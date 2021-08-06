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

//! Wrapper around `threadpool` with an attached RabbitMQ Connection used for consuming.
//! Each thread in the pool gets its own RabbitMq Channel/Consumer.
//! Each instance of a threadpool shares one RabbitMq connection amongst all of its threads.

use std::{cell::RefCell, sync::Arc};

use async_amqp::LapinAsyncStdExt;
use async_std::task;
use channel::{Receiver, Sender};
use futures::StreamExt;
use lapin::{
	message::Delivery,
	options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
	types::FieldTable,
	Connection, ConnectionProperties, Consumer,
};
use threadpool::ThreadPool;

use crate::{db::BackgroundJob, error::*, runner::Event};

thread_local!(static CONSUMER: RefCell<Option<Consumer>> = Default::default());

pub struct ThreadPoolMq {
	conn: Arc<lapin::Connection>,
    queue_name: Arc<String>,
	pool: ThreadPool,
	tx: Sender<Event>,
	rx: Receiver<Event>,
}

impl ThreadPoolMq {
	pub fn with_name(name: &str, threads: usize, addr: &str, queue_name: Arc<String>) -> Result<Self, Error> {
		let conn = Arc::new(Connection::connect(addr, ConnectionProperties::default().with_async_std()).wait()?);
		let pool = ThreadPool::with_name(name.into(), threads);
		let (tx, rx) = channel::bounded(pool.max_count());
		Ok(Self { conn, queue_name, pool,tx, rx })
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
	    let queue_name = self.queue_name.clone();
        self.pool.execute(move || {
			if let Err(e) = run_job(&conn, &queue_name, tx, job) {
				log::error!("{:?}", e);
			}
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

	#[cfg(test)]
	pub fn join(&self) {
		self.pool.join()
	}

	#[cfg(test)]
	pub fn panic_count(&self) -> usize {
		self.pool.panic_count()
	}
}


// FIXME: There may be a better way to do this that avoids sending in the 'queue_name' as a string.
// This is part of the reason the string is stored as Arc<String>, to cut down on memory-storage
// since string would have to be clone on every thread `execute`, despite only needing the string
// once when the thread is first started.
// We could: Intern the strings, or store a global hashmap via `OnceCell` of all queue names.
// However, those options sound more extreme and unnecessary for this use-case.
//
//
/// Run the job, initializing the thread-local consumer if it has not been initialized
fn run_job<F>(conn: &lapin::Connection, queue: &str, tx: Sender<Event>, job: F) -> Result<(), Error>
where
	F: Send + 'static + FnOnce(BackgroundJob) -> Result<(), PerformError>,
{
	CONSUMER.with(|c| {
		// FIXME: not entirely sure if this is safe or the best way to do this. Seems pretty
		// finicky with this Option<>, but it works fine afaict.
		let mut consumer = c.borrow_mut();
		let mut consumer = consumer.get_or_insert_with(|| {
			let chan = conn.create_channel().wait().ok().expect("Channel Creation should be flawless");
			chan.basic_qos(1, BasicQosOptions::default()).wait().expect("prototype");
			chan.basic_consume(queue, "", BasicConsumeOptions::default(), FieldTable::default())
				.wait()
				.ok()
				.expect("prototyping")
		});

		if let Some((data, delivery)) = next_job(tx, &mut consumer) {
			match job(data) {
				Ok(_) => {
					task::block_on(delivery.acker.ack(BasicAckOptions::default()))?;
				}
				Err(e) => {
					task::block_on(delivery.acker.nack(BasicNackOptions { requeue: true, ..Default::default() }))?;
					log::error!("Job failed to run {}", e);
					eprintln!("Job failed to run: {}", e);
				}
			}
		}
		Ok::<(), Error>(())
	})?;
	Ok(())
}

fn next_job(tx: Sender<Event>, consumer: &mut Consumer) -> Option<(BackgroundJob, Delivery)> {
	match get_next_job(consumer) {
		Ok(Some(d)) => {
			let _ = tx.send(Event::Working);
			Some(d)
		}
		Ok(None) => {
			let _ = tx.send(Event::NoJobAvailable);
			return None;
		}
		Err(e) => {
			let _ = tx.send(Event::ErrorLoadingJob(e));
			return None;
		}
	}
}

fn get_next_job(consumer: &mut Consumer) -> Result<Option<(BackgroundJob, Delivery)>, FetchError> {
	let delivery = task::block_on(consumer.next()).transpose()?.map(|(_, d)| d);
	let data: Option<BackgroundJob> =
		delivery.as_ref().map(|d| serde_json::from_slice(d.data.as_slice())).transpose()?;
	Ok(data.zip(delivery))
}
