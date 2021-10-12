// Copyright 2019-2021 Parity Technologies (UK) Ltd.
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

//! Periodically Crawls for storage entries.
//! This actor ensures the RabbitMQ queue for storage is kept saturated.

use crate::{
	actors::{
		workers::database::{DatabaseActor, GetState},
		ControlConfig,
	},
	database::{models::BlockModelDecoder, queries},
	error::Result,
};
use std::{convert::TryInto, marker::PhantomData};

use sa_work_queue::QueueHandle;
use substrate_archive_backend::{ApiAccess, ReadOnlyBackend, ReadOnlyDb};

use sc_client_api::backend;
use serde::de::DeserializeOwned;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use futures::StreamExt;

use xtra::prelude::*;

pub struct StorageEntries<Block, Runtime, Db, Client> {
	db: Address<DatabaseActor>,
	config: ControlConfig,
	_marker: PhantomData<(Block, Runtime, Db, Client)>,
}

impl<Block, Runtime, Db, Client> StorageEntries<Block, Runtime, Db, Client>
where
	Db: ReadOnlyDb + 'static,
	Block: BlockT + Unpin + DeserializeOwned,
	Runtime: ConstructRuntimeApi<Block, Client> + Send + Sync + 'static,
	Runtime::RuntimeApi: BlockBuilderApi<Block>
		+ sp_api::Metadata<Block>
		+ ApiExt<Block, StateBackend = backend::StateBackendFor<ReadOnlyBackend<Block, Db>, Block>>
		+ Send
		+ Sync
		+ 'static,
	Client: ApiAccess<Block, ReadOnlyBackend<Block, Db>, Runtime> + 'static,
	NumberFor<Block>: Into<u32> + From<u32> + Unpin,
	Block::Hash: Unpin,
	Block::Header: serde::de::DeserializeOwned,
{
	pub fn new(db: Address<DatabaseActor>, config: ControlConfig) -> Self {
		Self { db, config, _marker: PhantomData }
	}

	async fn queue_messages(&mut self, _: &mut Context<Self>, handle: QueueHandle) -> Result<()> {
		let mut conn = self.db.send(GetState::Conn).await??.conn();
		let nums = queries::missing_storage_blocks(&mut *conn).await?;
		log::info!("Restoring {} missing storage entries.", nums.len());
		let mut block_stream =
			queries::blocks_paginated(&mut *conn, nums.as_slice(), self.config.max_block_load.try_into()?);
		while let Some(Ok(page)) = block_stream.next().await {
			let jobs: Vec<crate::tasks::execute_block::Job<Block, Runtime, Client, Db>> =
				BlockModelDecoder::with_vec(page)?
					.into_iter()
					.map(|b| crate::tasks::execute_block::<Block, Runtime, Client, Db>(b.inner.block, PhantomData))
					.collect();

			sa_work_queue::JobExt::enqueue_batch(&handle, jobs).await?;
		}
		Ok(())
	}
}

#[async_trait::async_trait]
impl<Block: Send + Sync + 'static, Runtime: Send + Sync + 'static, Db: Send + Sync + 'static, Client: Send + Sync + 'static> Actor for StorageEntries<Block, Runtime, Db, Client> {}

pub struct QueueMessages(QueueHandle);
impl Message for QueueMessages {
	type Result = ();
}

#[async_trait::async_trait]
impl<Block, Runtime, Db, Client> Handler<QueueMessages> for StorageEntries<Block, Runtime, Db, Client>
where
	Db: ReadOnlyDb + 'static,
	Block: BlockT + Unpin + DeserializeOwned,
	Runtime: ConstructRuntimeApi<Block, Client> + Send + Sync + 'static,
	Runtime::RuntimeApi: BlockBuilderApi<Block>
		+ sp_api::Metadata<Block>
		+ ApiExt<Block, StateBackend = backend::StateBackendFor<ReadOnlyBackend<Block, Db>, Block>>
		+ Send
		+ Sync
		+ 'static,
	Client: ApiAccess<Block, ReadOnlyBackend<Block, Db>, Runtime> + 'static,
	NumberFor<Block>: Into<u32> + From<u32> + Unpin,
	Block::Hash: Unpin,
	Block::Header: serde::de::DeserializeOwned,
{
	async fn handle(&mut self, msg: QueueMessages, ctx: &mut Context<Self>) {
		let handle = msg.0;
		if handle.job_count() < self.config.max_block_load {
			if let Err(e) = self.queue_messages(ctx, handle).await {
				log::error!("{}", e);
			}
		}
	}
}
