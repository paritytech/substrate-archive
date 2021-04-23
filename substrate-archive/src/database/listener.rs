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

use futures::{future::BoxFuture, FutureExt, StreamExt};
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::{
	postgres::{PgConnection, PgListener, PgNotification},
	prelude::*,
};

use crate::error::Result;

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
	F: 'static + Send + Sync + for<'a> Fn(Notif, &'a mut PgConnection) -> BoxFuture<'a, Result<()>>,
{
	task: F,
	channels: Vec<Channel>,
	pg_url: String,
}

impl<F> Builder<F>
where
	F: 'static + Send + Sync + for<'a> Fn(Notif, &'a mut PgConnection) -> BoxFuture<'a, Result<()>>,
{
	pub fn new(url: &str, f: F) -> Self {
		Self { task: f, channels: Vec::new(), pg_url: url.to_string() }
	}

	pub fn listen_on(mut self, channel: Channel) -> Self {
		self.channels.push(channel);
		self
	}

	/// Spawns this listener which will work on its assigned tasks in the background
	pub async fn spawn(self, executor: &smol::Executor<'_>) -> Result<Listener> {
		let (tx, rx) = flume::bounded(1);

		let mut listener = PgListener::connect(&self.pg_url).await?;
		let channels = self.channels.iter().map(String::from).collect::<Vec<String>>();
		listener.listen_all(channels.iter().map(|s| s.as_ref())).await?;
		let mut conn = PgConnection::connect(&self.pg_url).await.unwrap();

		let fut = async move {
			let mut listener = listener.into_stream();
			loop {
				let mut listen_fut = listener.next().fuse();
				// pin_mut!(listen_fut);

				futures::select! {
					notif = listen_fut => {
						match notif {
							Some(Ok(v)) => {
								let fut = self.handle_listen_event(v, &mut conn);
								fut.await;
							},
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
			let timeout = smol::Timer::after(Duration::from_secs(1));
			futures::select! {
				_ = FutureExt::fuse(timeout) => {},
				notifs = listener.collect::<Vec<_>>().fuse() => {
					for msg in notifs {
						self.handle_listen_event(msg.unwrap(), &mut conn).await;
					}
				}
			}
		};

		executor.spawn(fut).detach();

		Ok(Listener { tx })
	}

	/// Handle a listen event from Postgres
	async fn handle_listen_event(&self, notif: PgNotification, conn: &mut PgConnection) {
		let payload: Notif = serde_json::from_str(notif.payload()).unwrap();
		(self.task)(payload, conn).await.unwrap();
	}
}

/// A Postgres listener which listens for events
/// on postgres channels using LISTEN/NOTIFY pattern
/// Dropping this will kill the listener,
pub struct Listener {
	// Shutdown signal
	tx: flume::Sender<()>,
}

impl Listener {
	pub fn builder<F>(pg_url: &str, f: F) -> Builder<F>
	where
		F: 'static + Send + Sync + for<'a> Fn(Notif, &'a mut PgConnection) -> BoxFuture<'a, Result<()>>,
	{
		Builder::new(pg_url, f)
	}

	pub async fn kill(&self) {
		let _ = self.tx.send_async(()).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures::{SinkExt, StreamExt};
	use sqlx::Connection;

	#[test]
	fn should_get_notifications() {
		crate::initialize();
		let _guard = crate::TestGuard::lock();
		smol::block_on(async move {
			let (tx, mut rx) = futures::channel::mpsc::channel(5);

			let _listener = Builder::new(&crate::DATABASE_URL, move |_, _| {
				let mut tx1 = tx.clone();
				async move {
					log::info!("Hello");
					tx1.send(()).await.unwrap();
					Ok(())
				}
				.boxed()
			})
			.listen_on(Channel::Blocks)
			.spawn()
			.await
			.unwrap();
			let mut conn = sqlx::PgConnection::connect(&crate::DATABASE_URL).await.expect("Connection dead");
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
				smol::Timer::after(Duration::from_millis(50)).await;
			}
			let mut counter: usize = 0;

			loop {
				let timeout = smol::Timer::after(Duration::from_millis(75));
				let mut timeout = FutureExt::fuse(timeout);
				futures::select!(
					_ = rx.next() => counter += 1,
					_ = timeout => break,
				)
			}

			assert_eq!(5, counter);
		});
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
