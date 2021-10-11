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

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use async_amqp::LapinAsyncStdExt;
use async_std::{future::timeout, task};
use flume::{Receiver, Sender};
use futures::{future, FutureExt, StreamExt};
use lapin::{
	message::Delivery,
	options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
	types::FieldTable,
	Channel, ChannelState, Connection, ConnectionProperties, Consumer,
};
use threadpool::ThreadPool;

use crate::{error::*, job::BackgroundJob, runner::Event};

thread_local!(static CONSUMER: ConsumerHandle = Default::default());

/// if a task takes more than this long, the channel will close.
/// The timeout is in milliseconds.
// const RABBITMQ_CHANNEL_TIMEOUT: u64 = 1800000;

#[derive(PartialEq, Clone, Debug)]
struct QueueOpts {
	queue_name: String,
	addr: String,
	prefetch: u16,
}

impl Default for QueueOpts {
	fn default() -> Self {
		Self { queue_name: "TASK_QUEUE".to_string(), addr: "amqp://localhost:5672".to_string(), prefetch: 1 }
	}
}

impl QueueOpts {
	fn create_connection(&self) -> Result<Connection, Error> {
		Ok(Connection::connect(&self.addr, ConnectionProperties::default().with_async_std()).wait()?)
	}
}

#[derive(Default)]
pub struct Builder {
	opts: QueueOpts,
	threads: Option<usize>,
	name: Option<String>,
}

impl Builder {
	pub fn queue_name<S: AsRef<str>>(mut self, name: S) -> Self {
		self.opts.queue_name = name.as_ref().to_string();
		self
	}

	pub fn addr<S: AsRef<str>>(mut self, addr: S) -> Self {
		self.opts.addr = addr.as_ref().to_string();
		self
	}

	pub fn prefetch(mut self, prefetch: u16) -> Self {
		self.opts.prefetch = prefetch;
		self
	}

	pub fn threads(mut self, threads: usize) -> Self {
		self.threads = Some(threads);
		self
	}

	pub fn name<S: AsRef<str>>(mut self, name: S) -> Self {
		self.name = Some(name.as_ref().to_string());
		self
	}

	pub fn build(self) -> Result<ThreadPoolMq, Error> {
		let conn = Arc::new(self.opts.create_connection()?);
		let pool = ThreadPool::with_name(
			self.name.unwrap_or_else(|| "work-queue".into()),
			self.threads.unwrap_or_else(num_cpus::get),
		);
		let (tx, rx) = flume::bounded(pool.max_count());

		Ok(ThreadPoolMq { conn, tx, rx, pool, queue_opts: Arc::new(self.opts) })
	}
}

pub struct ThreadPoolMq {
	conn: Arc<Connection>,
	queue_opts: Arc<QueueOpts>,
	pool: ThreadPool,
	tx: Sender<Event>,
	rx: Receiver<Event>,
}

impl ThreadPoolMq {
	pub fn builder() -> Builder {
		Default::default()
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
		let queue_opts = self.queue_opts.clone();
		self.pool.execute(move || {
			if let Err(e) = run_job(&conn, &queue_opts, tx, job) {
				log::error!("{}", e);
			}
		})
	}

	pub fn max_count(&self) -> usize {
		self.pool.max_count()
	}

	pub fn active_count(&self) -> usize {
		self.pool.active_count()
	}

	pub fn queued_count(&self) -> usize {
		self.pool.queued_count()
	}

	/// Get the receiving end of events sent from the threadpool
	pub fn events(&self) -> &Receiver<Event> {
		&self.rx
	}

	#[cfg(any(test, feature = "test_components"))]
	pub fn join(&self) {
		self.pool.join()
	}

	#[cfg(any(test, feature = "test_components"))]
	pub fn panic_count(&self) -> usize {
		self.pool.panic_count()
	}
}

#[derive(Clone)]
struct MaybeChannel(Rc<RefCell<Option<Channel>>>);

impl MaybeChannel {
	fn insert(&self, channel: Channel) {
		let mut this = self.0.borrow_mut();
		let _ = this.insert(channel);
	}
}

impl Default for MaybeChannel {
	fn default() -> Self {
		Self(Rc::new(RefCell::new(None)))
	}
}

// A handle to a consumer that by default is not initalized.
// mostly for convenience + clarity.
#[derive(Default, Clone)]
struct ConsumerHandle {
	inner: Rc<RefCell<Option<Consumer>>>,
	/// the channel this consumer belongs to.
	channel: MaybeChannel,
}

impl ConsumerHandle {
	fn current() -> ConsumerHandle {
		CONSUMER.with(|c| c.clone())
	}

	/// initialize the consumer if it is not already.
	fn init(&self, conn: &Connection, opts: &QueueOpts) -> Result<(), Error> {
		let mut this = self.inner.borrow_mut();
		if this.is_some() {
			return Ok(());
		}
		let chan = conn.create_channel().wait()?;
		chan.basic_qos(opts.prefetch, BasicQosOptions::default()).wait()?;
		log::debug!("Creating Channel for queue {}", &opts.queue_name);
		let consumer =
			chan.basic_consume(&opts.queue_name, "", BasicConsumeOptions::default(), FieldTable::default()).wait()?;
		let _ = this.insert(consumer);
		self.channel.insert(chan);
		Ok(())
	}

	/// Recover the channel if the task times out.
	fn recover(&self, _conn: &Connection, _opts: &QueueOpts) -> Result<(), Error> {
		if let Some(channel) = &*self.channel.0.borrow() {
			let id = channel.id();
			match channel.status().state() {
				ChannelState::Error => {
					log::info!("Recovering Channel");
					channel.basic_recover(Default::default()).wait()?
				},
				ChannelState::Closing => log::warn!("RabbitMQ Channel {} Closing", id),
				ChannelState::Closed => log::warn!("RabbitMQ Channel {} Closed", id), // this is not from an error.
				ChannelState::Connected => log::trace!("RabbitMQ Channel {} Connected", id),
				_ => ()
			};
			Ok(())
		} else {
			Ok(())
		}
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
fn run_job<F>(conn: &Connection, opts: &QueueOpts, tx: Sender<Event>, job: F) -> Result<(), Error>
where
	F: Send + 'static + FnOnce(BackgroundJob) -> Result<(), PerformError>,
{
	let handle = ConsumerHandle::current();
	handle.init(conn, opts)?;
	let mut consumer = handle.inner.borrow_mut();
	let mut consumer = consumer.as_mut().expect("Initialized handle must be Some; qed");
	if let Some((data, delivery)) = next_job(tx, &mut consumer) {
		match task::block_on(timed_job(job, data.clone())) {
			Ok(Ok(_)) => {
				task::block_on(delivery.acker.ack(BasicAckOptions::default()))?;
			}
			Ok(Err(e)) => {
				task::block_on(delivery.acker.nack(BasicNackOptions { requeue: false, ..Default::default() }))?;
				let job: BackgroundJob = serde_json::from_slice(&delivery.data)?;
				return Err(Error::Msg(format!("Job `{}` failed to run: {}", job.job_type, e)));
			}
			Err(Error::Timeout) => {
				log::warn!("task exceeded RabbitMQ timeout.");
				task::block_on(delivery.acker.nack(BasicNackOptions { requeue: false, ..Default::default() }))?;
				log::debug!("Failde Job Data {:?}", data);
				// would be nice to log task data here
				handle.recover(conn, opts)?;
			}
			e @ Err(_) => e??,
		}
	}
	Ok(())
}

type JobResult = Result<(), PerformError>;
/// In case a task takes greater than RABBITMQ_CHANNEL_TIMEOUT to execute,
/// we need to re-connect to the channel that disconnected.
/// In order to implement this timeout we should use our futures library (async-std),
/// but we dont want to spawn the job in spawn_blocking, because that would defeat the purpose
/// of our threadpool. Instead we spawn our timeout on async-stds executor, and select on our task & timeout.
async fn timed_job<F>(job: F, data: BackgroundJob) -> Result<JobResult, Error>
where
	F: Send + 'static + FnOnce(BackgroundJob) -> JobResult,
{
	let timeout_handle = task::sleep(Duration::from_millis(180_000));
	// NOTE:
	// Order matters here. If we place the timeout_handle _after_ executing `job`,
	// `job` as a blocking task will block the event loop (within task::block_on)
	// and our timeout future will never be spawned on the async_std _global_ executor.
	futures::select! {
		_ = task::spawn(timeout_handle).fuse() => Err(Error::Timeout),
		j = future::ready(job(data)) => Ok(j)
	}
}

fn next_job(tx: Sender<Event>, consumer: &mut Consumer) -> Option<(BackgroundJob, Delivery)> {
	match get_next_job(consumer) {
		Ok(Some(d)) => {
			let _ = tx.send(Event::Working);
			Some(d)
		}
		Ok(None) => {
			let _ = tx.send(Event::NoJobAvailable);
			None
		}
		Err(e) => {
			let _ = tx.send(Event::ErrorLoadingJob(e));
			None
		}
	}
}

fn get_next_job(consumer: &mut Consumer) -> Result<Option<(BackgroundJob, Delivery)>, FetchError> {
	let delivery =
		task::block_on(timeout(Duration::from_millis(10), consumer.next())).ok().flatten().transpose()?.map(|(_, d)| d);
	let data: Option<BackgroundJob> =
		delivery.as_ref().map(|d| serde_json::from_slice(d.data.as_slice())).transpose()?;
	Ok(data.zip(delivery))
}
