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
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

//! A PostgreSQL Listener
//! Listens to the specified channels,
//! and executes each tasks in each queue on each
//! listen wakeup.

use crate::error::Result;
use futures::{pin_mut, Future, FutureExt};
use serde::{Deserialize, Serialize};
use serde_aux::prelude::*;
use sqlx::postgres::{PgConnection, PgListener, PgNotification};
use sqlx::prelude::*;
use std::pin::Pin;
// use super::BlockModel;

/// A notification from Postgres about a new row
#[derive(PartialEq, Debug, Deserialize)]
pub struct Notif {
    pub table: Table,
    pub action: Action,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub id: i32,
}

#[derive(PartialEq, Debug, Deserialize)]
pub enum Table {
    #[serde(rename = "blocks")]
    Blocks,
    #[serde(rename = "storage")]
    Storage,
}

#[derive(PartialEq, Debug, Deserialize)]
pub enum Action {
    #[serde(rename = "INSERT")]
    Insert,
    #[serde(rename = "UPDATE")]
    Update,
    #[serde(rename = "DELETE")]
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
    F: 'static
        + Send
        + Sync
        + for<'a> Fn(
            Notif,
            &'a mut PgConnection,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>,
{
    task: F,
    disconnect:
        Box<dyn Send + Sync + Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>>,
    channels: Vec<Channel>,
    pg_url: String,
}

impl<F> Builder<F>
where
    F: 'static
        + Send
        + Sync
        + for<'a> Fn(
            Notif,
            &'a mut PgConnection,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>,
{
    pub fn new(url: &str, f: F) -> Self {
        Self {
            task: f,
            channels: Vec::new(),
            pg_url: url.to_string(),
            disconnect: Box::new(|| async move { Ok(()) }.boxed()),
        }
    }

    pub fn listen_on(mut self, channel: Channel) -> Self {
        self.channels.push(channel);
        self
    }

    /// Specify what to do in the event that this listener
    /// is disconnected from the Postgres database.
    pub fn on_disconnect<Fun>(mut self, fun: Fun) -> Self
    where
        Fun: Send + 'static + Sync + Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    {
        self.disconnect = Box::new(fun);
        self
    }

    /// Spawns this listener which will work on its assigned tasks in the background
    pub async fn spawn(self) -> Result<Listener> {
        let (tx, mut rx) = flume::bounded(1);

        let mut listener = PgListener::connect(&self.pg_url).await?;
        let channels = self
            .channels
            .iter()
            .map(|c| String::from(c))
            .collect::<Vec<String>>();
        listener
            .listen_all(channels.iter().map(|s| s.as_ref()))
            .await?;
        let mut conn = PgConnection::connect(&self.pg_url).await.unwrap();

        let fut = async move {
            loop {
                let listen_fut = listener.try_recv().fuse();
                pin_mut!(listen_fut);

                futures::select! {
                    notif = listen_fut => {
                        match notif {
                            Ok(Some(v)) => {
                                let fut = self.handle_listen_event(v, &mut conn);
                                fut.await;
                            },
                            Ok(None) => {
                                let fut = (self.disconnect)();
                                fut.await.unwrap()
                            },
                            Err(e) => {
                                log::error!("{:?}", e);
                            }
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
                    // complete => break,
                };
            }
            listener.unlisten_all().await.unwrap();
        };

        smol::Task::spawn(fut).detach();

        Ok(Listener { tx })
    }

    /// Handle a listen event from Postges
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
        F: 'static
            + Send
            + Sync
            + for<'a> Fn(
                Notif,
                &'a mut PgConnection,
            ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>,
    {
        Builder::new(pg_url, f)
    }

    #[allow(unused)]
    pub fn kill(&self) {
        let _ = self.tx.send(());
    }

    pub async fn kill_async(&self) {
        let _ = self.tx.send_async(()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{SinkExt, StreamExt};
    use once_cell::sync::Lazy;
    use sqlx::{Connection, Executor};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::sync::{Mutex, MutexGuard};

    static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    struct TestGuard<'a>(MutexGuard<'a, ()>);
    impl<'a> TestGuard<'a> {
        fn lock() -> Self {
            TestGuard(TEST_MUTEX.lock().expect("Test mutex panicked"))
        }
    }

    impl<'a> Drop for TestGuard<'a> {
        fn drop(&mut self) {
            smol::block_on(async move {
                let mut conn = crate::PG_POOL.acquire().await.unwrap();
                conn.execute(
                    "
                    TRUNCATE TABLE metadata CASCADE;
                    TRUNCATE TABLE storage CASCADE;
                    TRUNCATE TABLE blocks CASCADE;
                    -- TRUNCATE TABLE _background_tasks
                    ",
                )
                .await
                .unwrap();
            });
        }
    }

    #[test]
    fn should_get_notifications() {
        crate::initialize();
        let _guard = TestGuard::lock();
        smol::block_on(async move {
            let (tx, mut rx) = futures::channel::mpsc::channel(5);

            let listener = Builder::new(&crate::DATABASE_URL, move |_, _| {
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
            let mut conn = sqlx::PgConnection::connect(&crate::DATABASE_URL)
                .await
                .expect("Connection dead");
            let json = serde_json::json!({
                "table": "blocks",
                "action": "INSERT",
                "id":  1337
            })
            .to_string();
            for _ in 0usize..5usize {
                sqlx::query("SELECT pg_notify('blocks_update', $1)")
                    .bind(json.clone())
                    .execute(&mut conn)
                    .await
                    .expect("Could not exec notify query");
                smol::Timer::new(std::time::Duration::from_millis(50)).await;
            }
            let mut counter: usize = 0;

            loop {
                let mut timeout = smol::Timer::new(std::time::Duration::from_millis(75)).fuse();
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
            "id":  1337
        });

        let notif: Notif = serde_json::from_value(json).unwrap();

        assert_eq!(
            Notif {
                table: Table::Blocks,
                action: Action::Insert,
                id: 1337
            },
            notif
        );
    }
}
