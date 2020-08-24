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

use serde::{Serialize, Deserialize};
use std::pin::Pin;
use futures::{pin_mut, stream::StreamExt, Future, FutureExt};
use sqlx::postgres::{PgListener, PgNotification};
use crate::error::Result;

/// Channel encompassing any update to any table
/// const TABLE_UPDATE_CHAN: &str = "table_update";

pub enum Channel {
    /// Listen on the blocks table for new INSERTS
    Blocks = "blocks_update",
}

pub enum ChannelData {
    Block(Vec<u8>),
}

#[derive(Serialize, Deserialize)]
struct ListenEvent {
    table: String,
    action: String,
    data: serde_json::Value,
}

pub struct Builder {
    tasks: Vec<Box<dyn Send + Sync + Fn(ChannelData) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>>>,
    disconnect: Box<dyn Send + Sync + Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>>,
    channels: Vec<Channel>,
    pg_url: String,
}

impl Builder {

    pub fn new(url: &str) -> Self {
        Self {
            tasks: Vec::new(),
            channels: Vec::new(),
            pg_url: url.to_string(),
            disconnect: Box::new(|| { async move { Ok(()) }.boxed() })
        }
    }

    pub fn listen_on(mut self, channel: Channel) -> Self {
        self.channels.push(channel);
        self
    }

    /// Add a task to execute on listen event
    pub fn add_task<F>(mut self, task: F) -> Self
    where
        F: Send + 'static + Sync + Fn(ChannelData) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
    {
        self.tasks.push(Box::new(task));
        self
    }

    /// Specify what to do in the event that this listener
    /// is disconnected from the Postgres database.
    pub fn on_disconnect<F>(mut self, fun: F) -> Self
    where
        F: Send + 'static + Sync + Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>>

    {
        self.disconnect = Box::new(fun);
        self
    }

    /// Spawns this listener which will work on its assigned tasks in the background
    pub async fn spawn(self) -> Result<Listener> {
        let (tx, mut rx) = flume::bounded(1);

        let mut listener = PgListener::connect(&self.pg_url).await?;
        listener.listen_all(self.channels).await?;

        let fut = async move {
            loop {
                let listen_fut = listener.try_recv().fuse();
                pin_mut!(listen_fut);

                futures::select! {
                    notif = listen_fut => {
                        match notif {
                            Ok(Some(v)) => { 
                                let fut = self.handle_listen_event(v);
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
                    r = rx.recv_async() => break,
                    // complete => break,
                };
            }
            println!("I'm wrapping up already?");
            listener.unlisten(TABLE_UPDATE_CHAN).await.unwrap();
        };
        
        smol::Task::spawn(fut).detach();
        
        Ok(Listener { tx })
    }

    /// Handle a listen event from Postgers
    async fn handle_listen_event(&self, notif: PgNotification) {
        for task in self.tasks.iter() {
            log::info!("Got a Notification: {:?}", notif);
            task(ChannelData::Block(Vec::new())).await.unwrap();
        }
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
    pub fn builder(pg_url: &str) -> Builder {
        Builder::new(pg_url)
    }

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
    use once_cell::sync::Lazy;
    use std::sync::{Mutex, MutexGuard};
    use sqlx::{Executor, Connection};
    use futures::SinkExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    };

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
                conn.execute("
                    TRUNCATE TABLE metadata CASCADE;
                    TRUNCATE TABLE storage CASCADE;
                    TRUNCATE TABLE blocks CASCADE;
                    -- TRUNCATE TABLE _background_tasks
                    "
                ).await.unwrap();
            });
        }
    }

    #[test]
    fn should_get_notifications() {
        crate::initialize();
        let _guard = TestGuard::lock();
        smol::block_on(async move {
            let (tx, mut rx) = futures::channel::mpsc::channel(5);

            let listener = Builder::new(&crate::DATABASE_URL)
                .listen_on(Channel::Blocks)
                .add_task(move |_| {
                    let mut tx1 = tx.clone();
                    async move {
                        log::info!("Hello from a task");
                        let _ = tx1.send(()).await;
                        Ok(())
                    }.boxed()
                })
                .spawn().await.unwrap();
            let mut conn = sqlx::PgConnection::connect(&crate::DATABASE_URL).await.expect("Connection dead");
            for _ in 0usize..5usize {
                conn.execute("SELECT pg_notify('table_update', 'test')").await.expect("Could not exec notify query");
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
}
