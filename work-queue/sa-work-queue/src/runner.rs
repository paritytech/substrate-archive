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

use std::{
	any::Any,
	panic::{catch_unwind, PanicInfo, RefUnwindSafe, UnwindSafe},
	sync::Arc,
	time::Duration,
};

use async_amqp::*;
use lapin::{
	options::{BasicQosOptions, QueueDeclareOptions},
	BasicProperties, Channel, Connection, ConnectionProperties, Queue,
};

use crate::{db::BackgroundJob, error::*, job::Job, registry::Registry, threadpool::ThreadPoolMq};

/// Builder pattern struct for the Runner
pub struct Builder<Env> {
	environment: Env,
	num_threads: Option<usize>,
	addr: String,
	registry: Registry<Env>,
	queue_name: Option<String>,
	/// Amount of time to wait until job is deemed a failure
	timeout: Option<Duration>,
}

impl<Env: 'static> Builder<Env> {
	/// Instantiate a new instance of the Builder
	pub fn new<S: Into<String>>(environment: Env, addr: S) -> Self {
		let addr: String = addr.into();
		Self { environment, addr, num_threads: None, registry: Registry::load(), queue_name: None, timeout: None }
	}
	
    ///  Register a job that hasn't or can't be registered by invoking the `register_job!` macro
	///
	/// Jobs that include generics must use this function in order to be registered with a runner.
	/// Jobs must be registered with every generic that is used.
	/// Jobs are available in the format `my_function_name::Job`.
	///
	///  # Example
	///  ```ignore
	///  RunnerBuilder::new(env, conn)
	///      .register_job::<resize_image::Job<String>>()
	///  ```
	///  Different jobs must be registered with different generics if they exist.
	///
	///  ```ignore
	///  RunnerBuilder::new((), conn)
	///     .register_job::<resize_image::Job<String>>()
	///     .register_job::<resize_image::Job<u32>>()
	///     .register_job::<resize_image::Job<MyStruct>()
	///  ```
	///
	pub fn register_job<T: Job + 'static + Send>(mut self) -> Self {
		self.registry.register_job::<T>();
		self
	}

	/// specify the amount of threads to run the threadpool with
	pub fn num_threads(mut self, threads: usize) -> Self {
		self.num_threads = Some(threads);
		self
	}

	/// Set a timeout in seconds.
	/// This timeout is the maximum amount of time coil will wait for a job to begin
	/// before returning an error.
	pub fn timeout(mut self, timeout: Duration) -> Self {
		self.timeout = Some(timeout);
		self
	}

	/// Set the name for the queue to use.
	/// Default: `TASK_QUEUE`
	pub fn queue_name(mut self, name: String) -> Self {
		self.queue_name = Some(name);
        self
	}

	/// Build the runner
	pub fn build(self) -> Result<Runner<Env>, Error> {
		let timeout = self.timeout.unwrap_or_else(|| std::time::Duration::from_secs(5));
		let conn = Connection::connect(&self.addr, ConnectionProperties::default().with_async_std()).wait()?;
		let queue_name = Arc::new(self.queue_name.unwrap_or("TASK_QUEUE".to_string()));
		let handle = QueueHandle::new(&conn, &queue_name)?;
    	let threadpool =
			ThreadPoolMq::with_name("coil-worker", self.num_threads.unwrap_or(num_cpus::get()), &self.addr, queue_name.clone())?;

		Ok(Runner {
			threadpool,
			conn,
			handle,
			environment: Arc::new(self.environment),
			registry: Arc::new(self.registry),
            queue_name,
			timeout,
		})
	}
}

/// Runner for background tasks.
/// Synchronous tasks are run in a threadpool.
pub struct Runner<Env> {
	threadpool: ThreadPoolMq,
	conn: Connection,
	handle: QueueHandle,
	environment: Arc<Env>,
	registry: Arc<Registry<Env>>,
	queue_name: Arc<String>,
	timeout: Duration,
}

#[derive(Debug)]
pub enum Event {
	/// Queues are currently working
	Working,
	/// No more jobs available in queue
	NoJobAvailable,
	/// An error occurred loading the job from the database
	ErrorLoadingJob(FetchError),
}

/// Thin wrapper over a 'Channel'
#[derive(Clone)]
pub struct QueueHandle {
	channel: Channel,
	queue: Queue,
}

impl QueueHandle {
	/// Create a new QueueHandle.
	fn new(connection: &Connection, queue: &str) -> Result<Self, Error> {
		let channel = connection.create_channel().wait().unwrap();
		channel.basic_qos(1, BasicQosOptions::default()).wait()?;
		let queue = channel
			.queue_declare(
				queue,
				QueueDeclareOptions { durable: true, ..Default::default() },
				Default::default(),
			)
			.wait()?;

		Ok(Self { channel, queue })
	}

	/// Push to the RabbitMQ
	pub(crate) async fn push(&self, payload: Vec<u8>) -> Result<(), lapin::Error> {
		let properties = BasicProperties::default()
			// two means persist delivery to disk
			.with_delivery_mode(2);
		self.channel.basic_publish("", self.queue.name().as_str(), Default::default(), payload, properties).await?;
		Ok(())
	}
}

// Methods which don't require `RefUnwindSafe`
impl<Env: 'static> Runner<Env> {
	/// Build the builder for `Runner`
	pub fn builder(env: Env, conn: &str) -> Builder<Env> {
		Builder::new(env, conn)
	}

	/// Get a RabbitMq Connection from the pool that the runner is using.
	pub fn connection(&self) -> &Connection {
		&self.conn
	}
}

/// NOTE: need to consume here
impl<Env: Send + Sync + RefUnwindSafe + 'static> Runner<Env> {
	/// Runs all the pending tasks in a loop
	/// Returns how many tasks are running as a result
	pub fn run_pending_tasks(&self) -> Result<(), FetchError> {
		let max_threads = self.threadpool.max_count();
		log::debug!("Max Threads: {}", max_threads);

		let mut pending_messages = 0;
		loop {
			let available_threads = max_threads - self.threadpool.active_count();

			let jobs_to_queue =
				if pending_messages == 0 { std::cmp::max(available_threads, 1) } else { available_threads };

			for _ in 0..jobs_to_queue {
				self.run_single_sync_job()
			}

			pending_messages += jobs_to_queue;
			match self.threadpool.events().recv_timeout(self.timeout) {
				Ok(Event::Working) => pending_messages -= 1,
				Ok(Event::NoJobAvailable) => return Ok(()),
				Ok(Event::ErrorLoadingJob(e)) => return Err(e),
				Err(channel::RecvTimeoutError::Timeout) => return Err(FetchError::Timeout.into()),
				Err(channel::RecvTimeoutError::Disconnected) => {
					log::warn!("Job sender disconnected!");
					return Err(FetchError::Timeout.into());
				}
			}
		}
	}

	/// Get a reference to the handler held by `Runner`
	pub fn handle(&self) -> &QueueHandle {
		&self.handle
	}

	/// Create a new handle, using the same connection as `Runner`, but on a unique channel.
	pub fn unique_handle(&self) -> Result<QueueHandle, Error> {
		QueueHandle::new(&self.conn, &self.queue_name)
	}

    pub fn job_count(&self) -> u32 {
        self.handle.queue.message_count() 
    }

    pub fn max_jobs(&self) -> usize {
        self.threadpool.max_count()     
    }

	fn run_single_sync_job(&self) {
		let env = Arc::clone(&self.environment);
		let registry = Arc::clone(&self.registry);

		self.get_single_job(move |job| {
			let perform_fn = registry
				.get(&job.job_type)
				.ok_or_else(|| PerformError::from(format!("Unknown job type {}", job.job_type)))?;
			perform_fn.perform(job.data, &env)
		});
	}

	fn get_single_job<F>(&self, fun: F)
	where
		F: FnOnce(BackgroundJob) -> Result<(), PerformError> + Send + UnwindSafe + 'static,
	{
		self.threadpool
			.execute(move |job| catch_unwind(|| fun(job)).map_err(|e| try_to_extract_panic_info(&e)).and_then(|r| r))
	}
}

fn try_to_extract_panic_info(info: &(dyn Any + Send + 'static)) -> PerformError {
	if let Some(x) = info.downcast_ref::<PanicInfo>() {
		format!("job panicked: {}", x).into()
	} else if let Some(x) = info.downcast_ref::<&'static str>() {
		format!("job panicked: {}", x).into()
	} else if let Some(x) = info.downcast_ref::<String>() {
		format!("job panicked: {}", x).into()
	} else {
		"job panicked".into()
	}
}

#[cfg(any(test, feature = "test_components"))]
impl<Env: Send + Sync + RefUnwindSafe + 'static> Runner<Env> {
	/// Wait for tasks to finish based on timeout
	/// this is mostly used for internal tests
	fn wait_for_all_tasks(&self, num_tasks: usize) -> Result<(), String> {
		let mut counter = 0;
		while counter < num_tasks {
			match self.threadpool.events().recv_timeout(std::time::Duration::from_secs(5)) {
				Ok(o) => log::info!("{:?}", o),
				Err(channel::RecvTimeoutError::Disconnected) => break,
				Err(channel::RecvTimeoutError::Timeout) => {
					panic!("Tasks timed out");
				}
			}
			counter += 1
		}
		self.threadpool.join();
		let panic_count = self.threadpool.panic_count();
		if panic_count == 0 {
			Ok(())
		} else {
			Err(format!("{} threads panicked", panic_count).into())
		}
	}
	/*
	/// Check for any jobs that may have failed
	pub async fn check_for_failed_jobs(&self) -> Result<(), FailedJobsError> {
		self.wait_for_all_tasks().unwrap();
		let num_failed = db::failed_job_count(&self.pg_pool).await.unwrap();
		if num_failed == 0 {
			Ok(())
		} else {
			Err(FailedJobsError::JobsFailed(num_failed))
		}
	}
	*/
}

#[cfg(test)]
mod test {
	use super::*;
	use async_std::task;

    // we just use the job_type field as ID for testing purposes
	// in reality `job_type` is the name of the function
	fn create_dummy_job(runner: &Runner<()>, id: &str) {
		let job =
			BackgroundJob { job_type: id.to_string(), data: serde_json::from_str("[1, 2, 3, 4, 5, 6, 7, 8]").unwrap() };
		let handle = runner.handle();
		task::block_on(handle.push(serde_json::to_vec(&job).unwrap())).unwrap();
	}

	fn runner() -> Runner<()> {
		crate::Runner::builder((), "amqp://localhost:5672")
			.num_threads(2)
			.timeout(std::time::Duration::from_secs(5))
			.build()
			.unwrap()
	}
    
	#[test]
	fn sync_jobs_are_locked_when_fetched() {
		crate::initialize();
		
        let runner = runner();
		create_dummy_job(&runner, "1");
		create_dummy_job(&runner, "2");

		runner.get_single_job(move |job| {
			assert_eq!(job.job_type, "1");
			Ok(())
		});

		runner.get_single_job(move |job| {
			assert_eq!(job.job_type, "2");
			Ok(())
		});
		runner.wait_for_all_tasks(2).unwrap();
	}
    
    #[test]
    fn jobs_are_deleted_when_successful() {
        crate::initialize();

        let runner = runner();
        create_dummy_job(&runner, "1");
        runner.get_single_job(tx.clone(), move |_| Ok(()));
        runner.wait_for_all_tasks(1).unwrap();
        let remaining_jobs = runner.handle().queue.message_count();
        assert_eq!(0, remaining_jobs);
    }
}
