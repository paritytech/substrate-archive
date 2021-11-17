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

use arc_swap::ArcSwap;
use async_std::task;
use sqlx::PgPool;
use std::{collections::HashMap, sync::Arc};
use xtra::prelude::*;

use desub::Decoder;

use crate::{
	actors::{
		workers::database::{DatabaseActor, GetState},
		SystemConfig,
	},
	database::{models::ExtrinsicsModel, queries},
	error::{ArchiveError, Result},
	types::BatchExtrinsics,
};

pub struct ExtrinsicsDecoder {
	pool: PgPool,
	addr: Address<DatabaseActor>,
	max_block_load: u32,
	decoder: Arc<Decoder>,
	/// Hashmap of spec version -> number of block that upgrades to that spec version
	upgrades: ArcSwap<HashMap<u32, u32>>,
}

impl ExtrinsicsDecoder {
	pub async fn new<B: Send + Sync, Db: Send + Sync>(
		config: &SystemConfig<B, Db>,
		addr: Address<DatabaseActor>,
	) -> Result<Self> {
		let max_block_load = config.control.max_block_load;
		let chain = config.persistent_config.chain();
		let pool = addr.send(GetState::Pool).await??.pool();
		let decoder = Arc::new(Decoder::new(chain));
		let mut conn = pool.acquire().await?;
		let upgrades = ArcSwap::from_pointee(queries::upgrade_blocks_from_spec(&mut conn, 0).await?);
		Ok(Self { pool, addr, max_block_load, decoder, upgrades })
	}

	async fn crawl_missing_extrinsics(&mut self) -> Result<()> {
		let mut conn = self.pool.acquire().await?;
		let blocks = queries::missing_extrinsic_blocks(&mut conn, self.max_block_load).await?;

		let versions: Vec<u32> =
			blocks.iter().filter(|b| self.decoder.has_version(&b.3)).map(|(_, _, _, v)| *v).collect();
		// above and below line are separate to let immutable ref to `self.decoder` to go out of scope.
		for version in versions.iter() {
			let metadata = queries::metadata(&mut conn, *version as i32).await?;
			// TODO: FIX EXPECT
			Arc::get_mut(&mut self.decoder)
				.expect("Actors guarantee one reference; qed")
				.register_version(*version, &metadata)?;
		}

		if self.upgrades.load().iter().max_by(|a, b| a.1.cmp(b.1)).map(|(_, v)| v)
			< blocks.iter().map(|&(_, _, _, v)| v).max().as_ref()
		{
			self.update_upgrade_blocks().await?;
		}

		let decoder = self.decoder.clone();
		let upgrades = self.upgrades.load().clone();
		let extrinsics =
			task::spawn_blocking(move || Ok::<_, ArchiveError>(Self::decode(&decoder, blocks, &upgrades))).await??;

		self.addr.send(BatchExtrinsics::new(extrinsics)).await?;
		Ok(())
	}

	fn decode(
		decoder: &Decoder,
		blocks: Vec<(u32, Vec<u8>, Vec<u8>, u32)>,
		upgrades: &HashMap<u32, u32>,
	) -> Result<Vec<ExtrinsicsModel>> {
		let mut extrinsics = Vec::new();
		for (number, hash, ext, spec) in blocks.into_iter() {
			if upgrades.get(&number).is_some() {
				todo!()
			} else {
				let ext = decoder.decode_extrinsics(spec, ext.as_slice())?;
				extrinsics.push(ExtrinsicsModel::new(hash, number, ext)?);
			}
		}
		Ok(extrinsics)
	}

	async fn update_upgrade_blocks(&self) -> Result<()> {
		let max_spec = *self.upgrades.load().iter().max_by(|a, b| a.1.cmp(b.1)).map(|(k, _)| k).unwrap_or(&0);
		let mut conn = self.pool.acquire().await?;
		let upgrade_blocks = queries::upgrade_blocks_from_spec(&mut conn, max_spec).await?;
		self.upgrades.rcu(move |upgrades| {
			let mut upgrades = HashMap::clone(upgrades);
			upgrades.extend(upgrade_blocks.iter());
			upgrades
		});
		Ok(())
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
