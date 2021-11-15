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

use futures::StreamExt;
use serde_json::Value;
use sqlx::PgPool;
use xtra::prelude::*;

use desub::Decoder;

use crate::{
	actors::{
		workers::database::{DatabaseActor, GetState},
		SystemConfig,
	},
	database::{models::ExtrinsicsModel, queries, DbConn},
	error::{ArchiveError, Result},
	types::BatchExtrinsics,
};

pub struct ExtrinsicsDecoder {
	pool: PgPool,
	addr: Address<DatabaseActor>,
	max_block_load: u32,
	decoder: Decoder,
}

impl ExtrinsicsDecoder {
	pub async fn new<B: Send + Sync, Db: Send + Sync>(
		config: &SystemConfig<B, Db>,
		addr: Address<DatabaseActor>,
	) -> Result<Self> {
		let max_block_load = config.control.max_block_load;
		let chain = config.persistent_config.chain();
		let pool = addr.send(GetState::Pool).await??.pool();
		let decoder = Decoder::new(chain);
		Ok(Self { pool, addr, max_block_load, decoder })
	}

	async fn crawl_missing_extrinsics(&mut self) -> Result<()> {
		let mut conn = self.pool.acquire().await?;
		let mut stream = queries::missing_extrinsic_blocks(&mut conn, self.max_block_load).await;

		let mut extrinsics = Vec::new();
		let mut inner_conn = self.pool.acquire().await?;
		while let Some(Ok((number, hash, ext, spec))) = stream.next().await {
			let ext = self.decode(number, ext.as_slice(), spec, &mut inner_conn).await?;
			extrinsics.push(ExtrinsicsModel::new(hash, number, ext)?);
		}
		self.addr.send(BatchExtrinsics::new(extrinsics)).await?;
		Ok(())
	}

	async fn decode(&mut self, _number: u32, ext: &[u8], spec: u32, conn: &mut DbConn) -> Result<Value> {
		if self.decoder.has_version(&spec) {
			self.decoder.decode_extrinsics(spec, ext).map_err(Into::into)
		} else {
			let metadata = queries::metadata(conn, spec as i32).await?;
			self.decoder.register_version(spec, metadata.as_slice())?;
			self.decoder.decode_extrinsics(spec, ext).map_err(Into::into)
		}
	}
}

#[async_trait::async_trait]
impl Actor for ExtrinsicsDecoder {}

pub struct Index;
impl Message for Index {
	type Result = ();
}

#[async_trait::async_trait]
impl Handler<Index> for ExtrinsicsDecoder {
	async fn handle(&mut self, _: Index, ctx: &mut Context<Self>) {
		match self.crawl_missing_extrinsics().await {
			Err(ArchiveError::Disconnected) => ctx.stop(),
			Ok(_) => {}
			Err(e) => log::error!("{:?}", e),
		}
	}
}
