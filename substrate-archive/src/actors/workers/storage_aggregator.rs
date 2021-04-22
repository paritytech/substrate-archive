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

use std::sync::Arc;
use xtra::prelude::*;

use crate::{
	actors::workers::database::DatabaseActor,
	error::Result,
	types::{BatchStorage, Die, Hash, Storage},
	wasm_tracing::Traces,
};

pub struct StorageAggregator<H: Send + Sync + 'static> {
	db: Address<DatabaseActor>,
	storage: Vec<Storage<H>>,
	traces: Vec<Traces>,
	executor: Arc<smol::Executor<'static>>,
}

impl<H: Hash> StorageAggregator<H> {
	pub fn new(db: Address<DatabaseActor>, executor: Arc<smol::Executor<'static>>) -> Self {
		Self { db, storage: Vec::with_capacity(500), traces: Vec::with_capacity(250), executor }
	}

	async fn handle_storage(&mut self, ctx: &mut Context<Self>) -> Result<()> {
		let storage = std::mem::replace(&mut self.storage, Vec::with_capacity(500));
		if !storage.is_empty() {
			log::info!("Indexing {} blocks of storage entries", storage.len());
			let send_result = self.db.send(BatchStorage::new(storage));
			// handle_while the actual insert is happening, not the send
			ctx.handle_while(self, send_result).await?;
		}
		Ok(())
	}

	async fn handle_traces(&mut self, ctx: &mut Context<Self>) -> Result<()> {
		let mut traces = std::mem::take(&mut self.traces);
		if !traces.is_empty() {
			log::info!("Inserting {} traces", traces.len());
			for trace in traces.drain(..) {
				let send_result = self.db.send(trace);
				ctx.handle_while(self, send_result).await?;
			}
		}
		std::mem::swap(&mut self.traces, &mut traces);
		Ok(())
	}
}

#[async_trait::async_trait]
impl<H: Send + Sync + 'static> Actor for StorageAggregator<H> {}

pub struct SendStorage;
impl Message for SendStorage {
	type Result = ();
}

#[async_trait::async_trait]
impl<H: Hash> Handler<SendStorage> for StorageAggregator<H> {
	async fn handle(&mut self, _: SendStorage, ctx: &mut Context<Self>) {
		if let Err(e) = self.handle_storage(ctx).await {
			log::error!("{:?}", e)
		}
	}
}

pub struct SendTraces;
impl Message for SendTraces {
	type Result = ();
}

#[async_trait::async_trait]
impl<H: Hash> Handler<SendTraces> for StorageAggregator<H> {
	async fn handle(&mut self, _: SendTraces, ctx: &mut Context<Self>) {
		if let Err(e) = self.handle_traces(ctx).await {
			log::error!("{:?}", e);
		}
	}
}

#[async_trait::async_trait]
impl<H: Hash> Handler<Storage<H>> for StorageAggregator<H> {
	async fn handle(&mut self, s: Storage<H>, _: &mut Context<Self>) {
		self.storage.push(s)
	}
}

#[async_trait::async_trait]
impl<H: Hash> Handler<Traces> for StorageAggregator<H> {
	async fn handle(&mut self, t: Traces, _: &mut Context<Self>) {
		self.traces.push(t)
	}
}

#[async_trait::async_trait]
impl<H: Hash> Handler<Die> for StorageAggregator<H> {
	async fn handle(&mut self, _: Die, ctx: &mut Context<Self>) {
		ctx.stop();
	}
}
