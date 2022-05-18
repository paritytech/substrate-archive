// Copyright 2018-2021 Parity Technologies (UK) Ltd.
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

//! A PostgreSQL Listener
//! Listens to the specified channels,
//! and executes each tasks in each queue on each
//! listen wakeup.

use std::{fmt::Display, str::FromStr, time::Duration};

use async_std::{
	future::timeout,
	task::{self, JoinHandle},
};
use futures::{future::BoxFuture, FutureExt, StreamExt};
use sa_work_queue::QueueHandle;
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::{
	postgres::{PgConnection, PgListener, PgNotification},
	prelude::*,
};

use crate::error::{ArchiveError, Result};

/// A notification from Postgres about a new row
#[derive(PartialEq, Debug, Deserialize)]
pub struct Notif {
	pub table: Table,
	pub action: Action,
	#[serde(deserialize_with = "deserialize_number_from_string")]
	pub block_num: i32,
}

fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
	D: Deserializer<'de>,
	T: FromStr + Deserialize<'de>,
	<T as FromStr>::Err: Display,
{
	#[derive(Deserialize)]
	#[serde(untagged)]
	enum StringOrInt<T> {
		String(String),
		Number(T),
	}

	match StringOrInt::<T>::deserialize(deserializer)? {
		StringOrInt::String(s) => s.parse::<T>().map_err(serde::de::Error::custom),
		StringOrInt::Number(i) => Ok(i),
	}
}

#[derive(PartialEq, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
	Blocks,
	Storage,
}

#[derive(PartialEq, Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Action {
	Insert,
	Update,
	Delete,
}

pub enum Channel {
	/// Listen on the blocks table for new INSERTS
	Blocks,
}

impl From<&Channel> for String {
	fn from(chan: &Channel) -> String {
		match chan {
			Channel::Blocks => "blocks_update".to_string(),
		}
	}
}

#[derive(Serialize, Deserialize)]
struct ListenEvent {
	table: String,
	action: String,
	data: serde_json::Value,
}

pub struct Builder<F>
where
	F: 'static + Send + Sync + for<'a> Fn(Notif, &'a mut PgConnection, &'a QueueHandle) -> BoxFuture<'a, Result<()>>,
{
	task: F,
	channels: Vec<Channel>,
	pg_url: String,
	queue_handle: QueueHandle,
}

impl<F> Builder<F>
where
	F: 'static + Send + Sync + for<'a> Fn(Notif, &'a mut PgConnection, &'a QueueHandle) -> BoxFuture<'a, Result<()>>,
{
	pub fn new(url: &str, queue_handle: QueueHandle, f: F) -> Self {
		Self { task: f, channels: Vec::new(), pg_url: url.to_string(), queue_handle }
	}

	#[must_use]
	pub fn listen_on(mut self, channel: Channel) -> Self {
		self.channels.push(channel);
		self
	}

	/// Spawns this listener which will work on its assigned tasks in the background
	pub async fn spawn(self) -> Result<Listener> {
		let (tx, rx) = flume::bounded(1);
		let pg_url = self.pg_url.clone();

		// NOTE: this part is not included in the main future in order to prevent missing messages.
		// Otherwise, it would be possible to spawn, immediately send a notification, which would be missed if we are not connected/listening yet.
		let mut listener = PgListener::connect(&pg_url).await?;
		let channels = self.channels.iter().map(String::from).collect::<Vec<String>>();
		listener.listen_all(channels.iter().map(|s| s.as_ref())).await?;

		let fut = async move {
			let mut conn = PgConnection::connect(&pg_url).await.unwrap();
			let mut listener = listener.into_stream();

			loop {
				let mut listen_fut = listener.next().fuse();

				futures::select! {
					notif = listen_fut => {
						match notif {
							Some(Ok(v)) => self.handle_listen_event(v, &mut conn, &self.queue_handle).await?,
							Some(Err(e)) => {
								log::error!("{:?}", e);
							},
							None => {
								break;
							},
						}
					},
					r = rx.recv_async() => {
						match r {
							Ok(_) => break,
							Err(e) => {
								log::warn!("Ending due to: {:?}", e);
							}
						}
					},
					complete => break,
				};
			}

			// collect the rest of the results, before exiting, as long as the collection completes
			// in a reasonable amount of time
			let gather_unfinished = || async {
				for msg in listener.collect::<Vec<_>>().await {
					self.handle_listen_event(msg?, &mut conn, &self.queue_handle).await?;
				}
				Ok::<(), ArchiveError>(())
			};
			if timeout(Duration::from_secs(1), gather_unfinished()).await.is_err() {
				log::warn!("clean-up notification collection timed out")
			}
			Ok::<(), ArchiveError>(())
		};

		let handle = Some(task::spawn(fut));
		Ok(Listener { tx, handle })
	}

	/// Handle a listen event from Postgres
	async fn handle_listen_event(
		&self,
		notif: PgNotification,
		conn: &mut PgConnection,
		queue_handle: &QueueHandle,
	) -> Result<()> {
		let payload: Notif = serde_json::from_str(notif.payload())?;
		(self.task)(payload, conn, queue_handle).await?;
		Ok(())
	}
}

/// A Postgres listener which listens for events
/// on postgres channels using LISTEN/NOTIFY pattern
/// Dropping this will kill the listener,
pub struct Listener {
	// Shutdown signal
	tx: flume::Sender<()>,
	handle: Option<JoinHandle<Result<()>>>,
}

impl Listener {
	pub fn builder<F>(pg_url: &str, queue_handle: QueueHandle, f: F) -> Builder<F>
	where
		F: 'static
			+ Send
			+ Sync
			+ for<'a> Fn(Notif, &'a mut PgConnection, &'a QueueHandle) -> BoxFuture<'a, Result<()>>,
	{
		Builder::new(pg_url, queue_handle, f)
	}

	pub async fn kill(&mut self) -> Result<()> {
		let _ = self.tx.send_async(()).await;
		if let Some(handle) = self.handle.take() {
			handle.await?;
		}
		Ok(())
	}
}

impl Drop for Listener {
	fn drop(&mut self) {
		if let Err(e) = task::block_on(self.kill()) {
			log::error!("failed to terminate listener {}", e)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures::StreamExt;
	use sa_work_queue::QueueHandle;
	use sqlx::Connection;

	#[test]
	fn should_get_notifications() -> Result<()> {
		crate::initialize();
		let _guard = test_common::TestGuard::lock();
		test_common::insert_dummy_sql();
		let queue_handle = QueueHandle::new(&test_common::AMQP_CONN, test_common::TASK_QUEUE).unwrap();

		let future = async move {
			let (tx, rx) = flume::bounded(5);
			let mut listener = Builder::new(&test_common::DATABASE_URL, queue_handle, move |_, _, _| {
				let tx1 = tx.clone();
				async move {
					log::info!("Hello");
					tx1.send_async(()).await.unwrap();
					Ok(())
				}
				.boxed()
			})
			.listen_on(Channel::Blocks)
			.spawn()
			.await?;

			let mut conn = sqlx::PgConnection::connect(&test_common::DATABASE_URL).await.expect("Connection dead");
			let json = serde_json::json!({
				"table": "blocks",
				"action": "INSERT",
				"block_num":  1337
			})
			.to_string();
			for _ in 0usize..5usize {
				sqlx::query("SELECT pg_notify('blocks_update', $1)")
					.bind(json.clone())
					.execute(&mut conn)
					.await
					.expect("Could not exec notify query");
			}
			let mut counter: usize = 0;

			let mut rx = rx.into_stream();
			loop {
				if timeout(Duration::from_millis(25), rx.next()).await.is_err() {
					break;
				}
				counter += 1;
			}
			assert_eq!(5, counter);
			listener.kill().await?;

			Ok::<(), ArchiveError>(())
		};
		task::block_on(future)
	}

	#[test]
	fn should_deserialize_into_block() {
		let json = serde_json::json!({
			"table": "blocks",
			"action": "INSERT",
			"block_num":  1337
		});

		let notif: Notif = serde_json::from_value(json).unwrap();

		assert_eq!(Notif { table: Table::Blocks, action: Action::Insert, block_num: 1337 }, notif);
	}
}
