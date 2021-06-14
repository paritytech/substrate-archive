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

//! Common Sql queries on Archive Database abstracted into rust functions

use std::convert::TryFrom;

use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};
use sqlx::PgConnection;

use sp_runtime::traits::Block as BlockT;

use crate::{database::models::BlockModel, error::Result};

/// Return type of queries that `SELECT version`
struct Version {
	version: i32,
}

/// Return type of queries that `SELECT missing_num ... FROM ... GENERATE_SERIES(a, z)`
pub struct Series {
	missing_num: Option<i32>,
}

/// Return type of queries that `SELECT MAX(int)`
struct Max {
	max: Option<i32>,
}

/// Return type of queries that `SELECT EXISTS`
struct DoesExist {
	exists: Option<bool>,
}

// Return type of queries that `SELECT block_num`
struct BlockNum {
	block_num: i32,
}

// Return type of queries that `SELECT data`
struct Bytes {
	data: Vec<u8>,
}

/// Get missing blocks from the relational database between numbers `min` and
/// MAX(block_num). LIMIT result to length `max_block_load`. The highest effective
/// value for `min` is i32::MAX.
pub(crate) async fn missing_blocks_min_max(
	conn: &mut PgConnection,
	min: u32,
	max_block_load: u32,
) -> Result<HashSet<u32>> {
	let min = i32::try_from(min).unwrap_or(i32::MAX);
	let max_block_load = i64::try_from(max_block_load).unwrap_or(i64::MAX);
	// Remove after launchbadge/sqlx#594 is fixed
	#[allow(clippy::toplevel_ref_arg)]
	Ok(sqlx::query_as!(
		Series,
		"SELECT missing_num
		FROM (SELECT MAX(block_num) AS max_num FROM blocks) max,
		GENERATE_SERIES($1, max_num) AS missing_num
		WHERE
		NOT EXISTS (SELECT id FROM blocks WHERE block_num = missing_num)
		ORDER BY missing_num ASC
		LIMIT $2",
		min,
		max_block_load
	)
	.fetch_all(conn)
	.await?
	.iter()
	.map(|t| t.missing_num.unwrap() as u32)
	.collect())
}

/// Get the maximum block number from the relational database
pub(crate) async fn max_block(conn: &mut PgConnection) -> Result<Option<u32>> {
	let max = sqlx::query_as!(Max, "SELECT MAX(block_num) FROM blocks").fetch_one(conn).await?;
	Ok(max.max.map(|v| v as u32))
}

/// Get a block by id from the relational database
pub(crate) async fn get_full_block_by_number(conn: &mut sqlx::PgConnection, block_num: i32) -> Result<BlockModel> {
	#[allow(clippy::toplevel_ref_arg)]
	sqlx::query_as!(
		BlockModel,
		"
        SELECT id, parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec
        FROM blocks
        WHERE block_num = $1
        ",
		block_num
	)
	.fetch_one(conn)
	.await
	.map_err(Into::into)
}

/// Check if the runtime version identified by `spec` exists in the relational database
pub(crate) async fn check_if_meta_exists(spec: u32, conn: &mut PgConnection) -> Result<bool> {
	let spec = match i32::try_from(spec) {
		Err(_) => return Ok(false),
		Ok(n) => n,
	};
	#[allow(clippy::toplevel_ref_arg)]
	let does_exist = sqlx::query_as!(DoesExist, r#"SELECT EXISTS(SELECT version FROM metadata WHERE version = $1)"#, spec)
		.fetch_one(conn)
		.await?;
	Ok(does_exist.exists.unwrap_or(false))
}

/// Check if the block identified by `hash` exists in the relational database
pub(crate) async fn has_block<H: AsRef<[u8]>>(hash: H, conn: &mut PgConnection) -> Result<bool> {
	let hash = hash.as_ref();
	#[allow(clippy::toplevel_ref_arg)]
	let does_exist = sqlx::query_as!(DoesExist, r#"SELECT EXISTS(SELECT 1 FROM blocks WHERE hash = $1)"#, hash)
		.fetch_one(conn)
		.await?;
	Ok(does_exist.exists.unwrap_or(false))
}

/// Get a list of block_numbers, out of the passed-in blocknumbers, which exist in the relational
/// database
pub(crate) async fn has_blocks(nums: &[u32], conn: &mut PgConnection) -> Result<Vec<u32>> {
	let nums: Vec<i32> = nums.iter().filter_map(|n| i32::try_from(*n).ok()).collect();
	#[allow(clippy::toplevel_ref_arg)]
	Ok(sqlx::query_as!(BlockNum, "SELECT block_num FROM blocks WHERE block_num = ANY ($1)", &nums,)
		.fetch_all(conn)
		.await?
		.into_iter()
		.map(|r| r.block_num as u32)
		.collect())
}

/// Get all the metadata versions stored in the relational database
pub(crate) async fn get_versions(conn: &mut PgConnection) -> Result<Vec<u32>> {
	#[allow(clippy::toplevel_ref_arg)]
	Ok(sqlx::query_as!(Version, "SELECT version FROM metadata")
		.fetch_all(conn)
		.await?
		.into_iter()
		.map(|r| r.version as u32)
		.collect())
}

pub(crate) async fn missing_storage_items(conn: &mut sqlx::PgConnection) -> Result<Vec<BlockModel>> {
	Ok(sqlx::query_as!(
		BlockModel,
		"
		SELECT * FROM blocks WHERE block_num IN (
			SELECT block_num
				FROM
				(SELECT block_num FROM blocks EXCEPT SELECT CAST(data->>'block_num' AS integer) FROM _background_tasks) AS b
				WHERE NOT EXISTS (SELECT FROM storage WHERE storage.block_num = b.block_num)
			)
		ORDER BY block_num"
	)
	.fetch_all(conn)
	.await?
	.into_iter()
	.collect())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		database::{
			models::{BlockModelDecoder, StorageModel},
			Database, Insert,
		},
		types::BatchBlock,
		TestGuard,
	};
	use sp_api::HeaderT;
	use sp_storage::StorageKey;

	use polkadot_service::{Block, Hash};

	fn setup_data_scheme() {
		let mock_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

		let blocks: Vec<BlockModel> =
			test_common::get_kusama_blocks().unwrap().drain(0..1000).map(BlockModel::from).collect();
		let blocks = BlockModelDecoder::<Block>::with_vec(blocks).unwrap();

		smol::block_on(async {
			let database = Database::new(crate::DATABASE_URL.to_string()).await.unwrap();
			// insert some dummy data to satisfy the foreign key constraint
			sqlx::query("INSERT INTO metadata (version, meta) VALUES ($1, $2)")
				.bind(26)
				.bind(mock_bytes.as_slice())
				.execute(&mut database.conn().await.unwrap())
				.await
				.unwrap();
			database.insert(BatchBlock::new(blocks.clone())).await.unwrap();

			// blocks 0 - 200 will be missing from the storage table
			let mock_storage = blocks[200..]
				.iter()
				.map(|b| {
					StorageModel::new(
						b.inner.block.hash(),
						*b.inner.block.header().number(),
						false,
						StorageKey(mock_bytes.clone()),
						None,
					)
				})
				.collect::<Vec<StorageModel<Hash>>>();

			database.insert(mock_storage).await.unwrap();
		});
		println!("Sleeping...");
		std::thread::sleep(std::time::Duration::from_secs(5));
	}

	#[test]
	fn should_get_missing_storage() {
		crate::initialize();
		let _guard = TestGuard::lock();
		let data = setup_data_scheme();
	}
}
