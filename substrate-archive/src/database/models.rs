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

//! Direct Database Type representations of types in `types.rs`
//! Only some types implemented, for convenience most types are already in their database model
//! equivalents

use std::{convert::TryInto, marker::PhantomData};

use chrono::{DateTime, Utc};
use codec::{Decode, Encode, Error as DecodeError};
use serde::{Deserialize, Serialize};
use sqlx::{types::Json, FromRow, PgConnection, Postgres};

use desub::{types::LegacyOrCurrentExtrinsic, Chain};
use sc_executor::RuntimeVersion;
use sp_runtime::{
	generic::SignedBlock,
	traits::{Block as BlockT, Header as HeaderT},
};
use sp_storage::{StorageData, StorageKey};

use crate::{
	error::{ArchiveError, Result},
	types::*,
};

/// Struct modeling data returned from database when querying for a block
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub struct BlockModel {
	pub id: i32,
	pub parent_hash: Vec<u8>,
	pub hash: Vec<u8>,
	pub block_num: i32,
	pub state_root: Vec<u8>,
	pub extrinsics_root: Vec<u8>,
	pub digest: Vec<u8>,
	pub ext: Vec<u8>,
	pub spec: i32,
}

impl BlockModel {
	pub fn into_block_and_spec<B: BlockT>(self) -> Result<(B, u32), DecodeError> {
		let block_num = Decode::decode(&mut (self.block_num as u32).encode().as_slice())?;
		let extrinsics_root = Decode::decode(&mut self.extrinsics_root.as_slice())?;
		let state_root = Decode::decode(&mut self.state_root.as_slice())?;
		let parent_hash = Decode::decode(&mut self.parent_hash.as_slice())?;
		let digest = Decode::decode(&mut self.digest.as_slice())?;
		let ext = Decode::decode(&mut self.ext.as_slice())?;
		let header = <B::Header as HeaderT>::new(block_num, extrinsics_root, state_root, parent_hash, digest);

		let spec = self.spec as u32;

		Ok((B::new(header, ext), spec))
	}
}

/// Helper struct for decoding block modeling data into block type.
pub struct BlockModelDecoder<B: BlockT> {
	_marker: PhantomData<B>,
}

impl<'a, B: BlockT> BlockModelDecoder<B> {
	/// With a vector of BlockModel
	pub fn with_vec(blocks: Vec<BlockModel>) -> Result<Vec<Block<B>>, DecodeError> {
		blocks
			.into_iter()
			.map(|b| {
				let (block, spec) = b.into_block_and_spec()?;
				let block = SignedBlock { block, justifications: None };
				Ok(Block::new(block, spec))
			})
			.collect()
	}
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StorageModel<Hash> {
	hash: Hash,
	block_num: u32,
	full_storage: bool,
	key: StorageKey,
	data: Option<StorageData>,
}

impl<Hash> StorageModel<Hash> {
	pub fn new(hash: Hash, block_num: u32, full_storage: bool, key: StorageKey, data: Option<StorageData>) -> Self {
		Self { hash, block_num, full_storage, key, data }
	}

	pub fn is_full(&self) -> bool {
		self.full_storage
	}

	pub fn block_num(&self) -> u32 {
		self.block_num
	}

	pub fn hash(&self) -> &Hash {
		&self.hash
	}

	pub fn key(&self) -> &StorageKey {
		&self.key
	}

	pub fn data(&self) -> Option<&StorageData> {
		self.data.as_ref()
	}
}

impl<Hash: Copy> From<Storage<Hash>> for Vec<StorageModel<Hash>> {
	fn from(original: Storage<Hash>) -> Vec<StorageModel<Hash>> {
		let hash = *original.hash();
		let block_num = original.block_num();
		let full_storage = original.is_full();
		original
			.changes
			.into_iter()
			.map(|changes| StorageModel::new(hash, block_num, full_storage, changes.0, changes.1))
			.collect::<Vec<StorageModel<Hash>>>()
	}
}

impl<Hash: Copy> From<BatchStorage<Hash>> for Vec<StorageModel<Hash>> {
	fn from(original: BatchStorage<Hash>) -> Vec<StorageModel<Hash>> {
		original.inner.into_iter().flat_map(Vec::<StorageModel<Hash>>::from).collect()
	}
}

#[derive(Debug, Serialize, FromRow)]
pub struct ExtrinsicsModel {
	pub id: Option<i32>,
	pub hash: Vec<u8>,
	pub number: i32,
	pub extrinsics: Json<Vec<LegacyOrCurrentExtrinsic>>,
}

impl ExtrinsicsModel {
	pub fn new(hash: Vec<u8>, number: u32, extrinsics: Vec<LegacyOrCurrentExtrinsic>) -> Result<Self> {
		let number = number.try_into()?;
		Ok(Self { id: None, hash, number, extrinsics: Json(extrinsics) })
	}
}

/// Config that is stored/restored in Postgres on every run.
/// This is needed to persist RabbitMq task-queue name between runs.
/// Archive version and timestamp included as extra metadata
/// that could be useful for debugging.
#[derive(FromRow, Debug, Clone)]
pub struct PersistentConfig {
	// internal SQL identifier.
	// required to be here when restoring the entire row from Postgres,
	// otherwise we get decoding errors. Also useful for ordering of elements in postgres.
	#[allow(unused)]
	id: i32,
	/// RabbitMQ Queue Name
	pub task_queue: String,
	/// Last time the archive was run
	pub last_run: DateTime<Utc>,
	/// Major version of this library.
	pub major: i32,
	/// Minor version of this library.
	pub minor: i32,
	/// Patch version of this library.
	pub patch: i32,
	/// The chain data this db is populated with
	pub chain: String,
	/// The genesis hash of the network the db is populated with.
	pub genesis_hash: Vec<u8>,
}

impl PersistentConfig {
	/// Get the config and update it if it exists. If not initialize config and return it.
	pub async fn fetch_and_update<H>(conn: &mut PgConnection, version: RuntimeVersion, genesis: H) -> Result<Self>
	where
		H: AsRef<[u8]>,
	{
		#[derive(FromRow)]
		struct DbName {
			current_database: String,
		}

		#[derive(FromRow)]
		struct Id {
			id: i32,
		}

		#[derive(FromRow)]
		struct StoredChain {
			chain: String,
		}

		// FIXME: No `query_as!` macro until https://github.com/launchbadge/sqlx/issues/1294#issuecomment-866618995
		let conf = sqlx::query_as::<Postgres, Self>(r#"SELECT * FROM _sa_config ORDER BY id LIMIT 1"#)
			.fetch_optional(&mut *conn)
			.await?;

		let last_run = Utc::now();
		let (major, minor, patch) = Self::get_version_tuple()?;

		let running_chain = version.spec_name.to_string();

		if conf.is_none() {
			let mut task_queue: String = sqlx::query_as::<Postgres, DbName>(r#"SELECT current_database()"#)
				.fetch_one(&mut *conn)
				.await?
				.current_database;
			// queue name is a combination of: database name, "-queue" and the current UTC timestamp.
			// This is to ensure some level of uniqueness if one server is running multiple
			// instances of archive for different chains.
			task_queue.push_str(&format!("-queue-{}", Utc::now().timestamp()));
			let id = sqlx::query_as::<Postgres, Id>(
				r#"INSERT INTO _sa_config (task_queue, last_run, major, minor, patch, chain, genesis_hash) VALUES($1, $2, $3, $4, $5, $6, $7) RETURNING id"#,
			)
			.bind(&task_queue)
			.bind(last_run)
			.bind(major)
			.bind(minor)
			.bind(patch)
			.bind(&running_chain)
			.bind(&genesis.as_ref())
			.fetch_one(&mut *conn)
			.await?
			.id;

			Ok(Self {
				id,
				task_queue,
				last_run,
				major,
				minor,
				patch,
				chain: running_chain,
				genesis_hash: genesis.as_ref().to_vec(),
			})
		} else {
			let stored_chain = sqlx::query_as::<Postgres, StoredChain>("SELECT chain FROM _sa_config")
				.fetch_one(&mut *conn)
				.await?
				.chain;
			if stored_chain.as_str() != running_chain {
				return Err(ArchiveError::MismatchedSpecName { expected: stored_chain, got: running_chain });
			}

			sqlx::query(r#"UPDATE _sa_config SET last_run = $1, major = $2, minor = $3, patch = $4"#)
				.bind(last_run)
				.bind(major)
				.bind(minor)
				.bind(patch)
				.execute(&mut *conn)
				.await?;
			Ok(conf.expect("Checked for none; qed"))
		}
	}

	fn get_version_tuple() -> Result<(i32, i32, i32)> {
		let version = env!("CARGO_PKG_VERSION");
		let version = semver::Version::parse(version)?;
		Ok((version.major.try_into()?, version.minor.try_into()?, version.patch.try_into()?))
	}

	pub(crate) fn chain(&self) -> Chain {
		match self.chain.to_ascii_lowercase().as_str() {
			"kusama" => Chain::Kusama,
			"polkadot" => Chain::Polkadot,
			"westend" => Chain::Westend,
			"centrifuge" => Chain::Centrifuge,
			"rococo" => Chain::Rococo,
			s => Chain::Custom(s.to_string()),
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use anyhow::Error;
	use async_std::task;
	use test_common::{TestGuard, PG_POOL};

	#[derive(FromRow)]
	struct TaskQueueQuery {
		task_queue: String,
	}

	#[test]
	fn config_should_persist() -> Result<(), Error> {
		crate::initialize();
		let _guard = TestGuard::lock();
		task::block_on(async {
			let mut conn = PG_POOL.acquire().await?;
			PersistentConfig::fetch_and_update(&mut *conn, Default::default(), vec![]).await?;
			let query = sqlx::query_as::<Postgres, TaskQueueQuery>("SELECT task_queue FROM _sa_config LIMIT 1")
				.fetch_one(&mut *conn)
				.await?;
			let conf = PersistentConfig::fetch_and_update(&mut *conn, Default::default(), vec![]).await?;
			assert_eq!(query.task_queue, conf.task_queue);
			Ok::<(), Error>(())
		})?;
		Ok(())
	}
}
