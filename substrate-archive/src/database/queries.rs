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
use sqlx::PgConnection;

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
		"
		SELECT missing_num
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
	Ok(sqlx::query_as!(BlockNum, "SELECT block_num FROM blocks WHERE block_num = ANY ($1)", &nums)
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

pub(crate) async fn missing_storage_blocks(conn: &mut sqlx::PgConnection) -> Result<Vec<u32>> {
	let blocks: Vec<u32> = sqlx::query_as!(BlockNum,
	   r#"SELECT block_num AS "block_num!" FROM
            (SELECT block_num FROM blocks EXCEPT
                SELECT (HEX_TO_INT(LTRIM(data->'block'->'header'->>'number', '0x'))) AS block_num FROM _background_tasks WHERE job_type = 'execute_block') AS maybe_missing
        WHERE NOT EXISTS
	        (SELECT block_num FROM storage WHERE storage.block_num = maybe_missing.block_num)
		ORDER BY block_num"#
	)
		.fetch_all(conn)
		.await?
		.into_iter()
		.map(|r| r.block_num as u32)
		.collect();
	Ok(blocks)
}

/// Get full blocks in pages
pub(crate) async fn blocks_paginated(
	conn: &mut sqlx::PgConnection,
	nums: &[u32],
	limit: i64,
	page: i64,
) -> Result<Vec<BlockModel>> {
	let nums: Vec<i32> = nums.iter().filter_map(|n| i32::try_from(*n).ok()).collect();
	let mut query = String::from("SELECT * FROM blocks WHERE block_num IN (");
	for (i, num) in nums.iter().enumerate() {
		itoa::fmt(&mut query, *num)?;
		if i != nums.len() - 1 {
			query.push(',');
		}
	}
	query.push_str(") ORDER BY block_num LIMIT $1 OFFSET $2");
	Ok(sqlx::query_as(query.as_str()).bind(limit).bind(page * limit).fetch_all(conn).await?)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		database::{
			models::{BlockModelDecoder, StorageModel},
			Database,
		},
		types::BatchBlock,
		TestGuard,
	};
	use anyhow::Error;
	use sp_api::{BlockT, HeaderT};
	use sp_storage::StorageKey;
	use sqlx::{pool::PoolConnection, postgres::Postgres};

	use polkadot_service::{Block, Hash};

	// kusama block height dataset starts at this number
	const BLOCK_START: usize = 3_000_000;

	// Setup a data scheme such that:
	// - blocks 0 - 200 will be missing from the `storage` table
	// - blocks 100 - 200 will be present in the `_background_tasks` table
	// - all blocks will be present in the `blocks` table
	async fn setup_data_scheme() -> Result<PoolConnection<Postgres>, Error> {
		let mock_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

		let blocks: Vec<BlockModel> = test_common::get_kusama_blocks()?.drain(0..1000).map(BlockModel::from).collect();
		let blocks = BlockModelDecoder::<Block>::with_vec(blocks)?;

		let database = Database::new(crate::DATABASE_URL.to_string()).await?;
		// insert some dummy data to satisfy the foreign key constraint
		sqlx::query("INSERT INTO metadata (version, meta) VALUES ($1, $2)")
			.bind(26)
			.bind(mock_bytes.as_slice())
			.execute(&mut database.conn().await?)
			.await?;
		database.insert(BatchBlock::new(blocks.clone())).await?;

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
		database.insert(mock_storage).await?;

		crate::database::tests::enqueue_mock_jobs(&blocks[100..200], &mut database.conn().await.unwrap()).await?;
		Ok(database.conn().await?)
	}

	#[test]
	fn should_get_missing_storage() -> Result<(), Error> {
		crate::initialize();
		let _guard = TestGuard::lock();
		let mut conn = smol::block_on(setup_data_scheme())?;
		let items = smol::block_on(missing_storage_blocks(&mut conn))?;

		assert_eq!(items.len(), 100);
		assert_eq!(items.iter().min(), Some(&3_000_001u32));
		assert_eq!(items.iter().max(), Some(&3_000_100u32));
		Ok(())
	}

	#[test]
	fn should_paginate_blocks() -> Result<(), Error> {
		crate::initialize();
		let _guard = TestGuard::lock();
		smol::block_on(async {
			let mut conn = setup_data_scheme().await?;
			let mut block_nums: Vec<u32> =
				vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21];
			block_nums.iter_mut().for_each(|b| {
				*b += BLOCK_START as u32;
			});
			let blocks = blocks_paginated(&mut conn, block_nums.as_slice(), 7, 0)
				.await?
				.iter()
				.map(|b| (b.block_num - BLOCK_START as i32))
				.collect::<Vec<i32>>();
			assert_eq!(vec![1, 2, 3, 4, 5, 6, 7], blocks);

			let blocks = blocks_paginated(&mut conn, block_nums.as_slice(), 7, 1)
				.await?
				.iter()
				.map(|b| b.block_num - BLOCK_START as i32)
				.collect::<Vec<i32>>();
			assert_eq!(vec![8, 9, 10, 11, 12, 13, 14], blocks);

			let blocks = blocks_paginated(&mut conn, block_nums.as_slice(), 7, 2)
				.await?
				.iter()
				.map(|b| b.block_num - BLOCK_START as i32)
				.collect::<Vec<i32>>();
			assert_eq!(vec![15, 16, 17, 18, 19, 20, 21], blocks);
			Ok(())
		})
	}
}
