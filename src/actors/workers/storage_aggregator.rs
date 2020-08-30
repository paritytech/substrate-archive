// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

//! Module that accepts individual storage entries and wraps them up into batch requests for
//! Postgres

use super::{ActorPool, DatabaseActor};
use crate::actors::msg::VecStorageWrap;
use crate::error::Result;
use crate::types::Storage;
use sp_runtime::traits::Block as BlockT;
use xtra::prelude::*;

pub struct StorageAggregator<B: BlockT + Unpin> {
    db: Address<ActorPool<DatabaseActor<B>>>,
    storage: Vec<Storage<B>>,
}

impl<B: BlockT + Unpin> StorageAggregator<B>
where
    B::Hash: Unpin,
{
    pub fn new(db: Address<ActorPool<DatabaseActor<B>>>) -> Self {
        Self {
            db,
            storage: Vec::with_capacity(500),
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Actor for StorageAggregator<B>
where
    B::Hash: Unpin,
{
    async fn started(&mut self, ctx: &mut Context<Self>) {
        let addr = ctx.address().expect("Actor just started");
        smol::Task::spawn(async move {
            loop {
                smol::Timer::new(std::time::Duration::from_secs(1)).await;
                if let Err(_) = addr.send(SendStorage).await {
                    break;
                }
            }
        })
        .detach();
    }

    async fn stopped(&mut self, _: &mut Context<Self>) {
        let len = self.storage.len();
        let storage = std::mem::take(&mut self.storage);
        // insert any storage left in queue
        let task = self.db.send(VecStorageWrap(storage).into()).await;
        match task {
            Err(e) => {
                log::info!("{} storage entries will be missing, {:?}", len, e);
            }
            Ok(v) => {
                log::info!("waiting for last storage insert...");
                v.await;
                log::info!("storage inserted");
            }
        }
    }
}

struct SendStorage;
impl Message for SendStorage {
    type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<SendStorage> for StorageAggregator<B>
where
    B::Hash: Unpin,
{
    async fn handle(&mut self, _: SendStorage, _: &mut Context<Self>) {
        let storage = std::mem::take(&mut self.storage);
        if !storage.is_empty() {
            log::info!("Indexing storage {} bps", storage.len());
            if let Err(e) = self.db.send(VecStorageWrap(storage).into()).await {
                log::error!("{:?}", e);
            }
        }
    }
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<Storage<B>> for StorageAggregator<B>
where
    B::Hash: Unpin,
{
    async fn handle(&mut self, s: Storage<B>, _: &mut Context<Self>) {
        self.storage.push(s)
    }
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<super::Die> for StorageAggregator<B>
where
    B::Hash: Unpin,
{
    async fn handle(&mut self, _: super::Die, ctx: &mut Context<Self>) -> Result<()> {
        ctx.stop();
        Ok(())
    }
}
