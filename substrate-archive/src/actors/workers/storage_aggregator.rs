// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use xtra::prelude::*;

use sp_runtime::traits::Block as BlockT;
use std::sync::Arc;

use crate::{
	actors::workers::database::DatabaseActor,
	error::Result,
	types::{BatchStorage, Die, Storage},
	wasm_tracing::Traces,
};

pub struct StorageAggregator<B: BlockT + Unpin> {
	db: Address<DatabaseActor<B>>,
	storage: Vec<Storage<B>>,
	traces: Vec<Traces>,
	executor: Arc<smol::Executor<'static>>,
}

impl<B: BlockT + Unpin> StorageAggregator<B>
where
	B::Hash: Unpin,
{
	pub fn new(db: Address<DatabaseActor<B>>, executor: Arc<smol::Executor<'static>>) -> Self {
		Self { db, storage: Vec::with_capacity(500), traces: Vec::with_capacity(250), executor }
	}

	async fn handle_storage(&mut self, _: &mut Context<Self>) -> Result<()> {
		let storage = std::mem::replace(&mut self.storage, Vec::with_capacity(500));
		if !storage.is_empty() {
			log::info!("Indexing {} blocks of storage entries", storage.len());
			let now = std::time::Instant::now();
			self.db.send(BatchStorage::new(storage)).await?;
			log::debug!("Took {:?} to send & insert storage", now.elapsed());
		}
		Ok(())
	}

	async fn handle_traces(&mut self, _: &mut Context<Self>) -> Result<()> {
		let mut traces = std::mem::take(&mut self.traces);
		if !traces.is_empty() {
			log::info!("Inserting {} traces", traces.len());
			for trace in traces.drain(..) {
				self.db.send(trace).await?;
			}
		}
		std::mem::swap(&mut self.traces, &mut traces);
		Ok(())
	}
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Actor for StorageAggregator<B>
where
	B::Hash: Unpin,
{
	async fn started(&mut self, ctx: &mut Context<Self>) {
		let addr = ctx.address().expect("Actor just started");
		self.executor
			.spawn(async move {
				loop {
					smol::Timer::after(std::time::Duration::from_secs(1)).await;
					if addr.send(SendStorage).await.is_err() {
						break;
					}
					if addr.send(SendTraces).await.is_err() {
						break;
					}
				}
			})
			.detach();
	}

	async fn stopped(&mut self) {
		let len = self.storage.len();
		let storage = std::mem::take(&mut self.storage);
		// insert any storage left in queue
		let task = self.db.send(BatchStorage::new(storage)).await;

		match task {
			Err(e) => log::info!("{} storage entries will be missing, {:?}", len, e),
			Ok(_) => log::info!("storage inserted"),
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
	async fn handle(&mut self, _: SendStorage, ctx: &mut Context<Self>) {
		if let Err(e) = self.handle_storage(ctx).await {
			log::error!("{:?}", e)
		}
	}
}

struct SendTraces;
impl Message for SendTraces {
	type Result = ();
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<SendTraces> for StorageAggregator<B>
where
	B::Hash: Unpin,
{
	async fn handle(&mut self, _: SendTraces, ctx: &mut Context<Self>) {
		if let Err(e) = self.handle_traces(ctx).await {
			log::error!("{:?}", e);
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
impl<B: BlockT + Unpin> Handler<Traces> for StorageAggregator<B>
where
	B::Hash: Unpin,
{
	async fn handle(&mut self, t: Traces, _: &mut Context<Self>) {
		self.traces.push(t)
	}
}

#[async_trait::async_trait]
impl<B: BlockT + Unpin> Handler<Die> for StorageAggregator<B>
where
	B::Hash: Unpin,
{
	async fn handle(&mut self, _: Die, ctx: &mut Context<Self>) {
		ctx.stop();
	}
}
