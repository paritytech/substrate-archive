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

use async_stream::try_stream;
use futures::Stream;
use hashbrown::HashSet;
use itertools::Itertools;
use sqlx::PgConnection;
use std::collections::HashMap;

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

// Return type of queries that `SELECT block_num, ext`
struct BlockExtrinsics {
	block_num: i32,
	hash: Vec<u8>,
	ext: Vec<u8>,
	spec: i32,
}

/// Return type of queries that `SELECT meta`
struct Meta {
	pub meta: Vec<u8>,
}

/// Return type of queries that `SELECT block_num, spec`
#[derive(Copy, Clone)]
struct BlockNumSpec {
	block_num: i32,
	spec: i32,
}

/// Return tye of queries which `SELECT present, past, metadata, past_metadata`
struct PastAndPresentVersion {
	pub present: i32,
	pub past: Option<i32>,
	pub metadata: Vec<u8>,
	pub past_metadata: Option<Vec<u8>>,
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

/// Get metadata according to spec version.
pub async fn metadata(conn: &mut PgConnection, spec: i32) -> Result<Vec<u8>> {
	sqlx::query_as!(Meta, "SELECT meta FROM metadata WHERE version = $1", spec)
		.fetch_one(conn)
		.await
		.map_err(Into::into)
		.map(|m| m.meta)
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
	let blocks: Vec<u32> = sqlx::query_as!(
		BlockNum,
		r#"
         SELECT block_num FROM blocks
         WHERE NOT EXISTS
            (SELECT block_num FROM storage WHERE storage.block_num = blocks.block_num) AND block_num > (SELECT MAX(block_num) FROM storage)
		AND block_num >= (SELECT MAX(block_num) from storage)
        ORDER BY block_num ASC
		LIMIT 1000;
        "#
	)
	.fetch_all(conn)
	.await?
	.into_iter()
	.map(|r| r.block_num as u32)
	.collect();
	Ok(blocks)
}

/// Get full blocks in pages
pub(crate) fn blocks_paginated<'a>(
	conn: &'a mut sqlx::PgConnection,
	nums: &'a [u32],
	limit: usize,
) -> impl Stream<Item = Result<Vec<BlockModel>>> + 'a {
	let nums: Vec<i32> = nums.iter().filter_map(|n| i32::try_from(*n).ok()).collect();

	Box::pin(try_stream! {
		for page in nums.chunks(limit) {
			let mut query = String::from("SELECT * FROM blocks WHERE block_num IN (");
			for (i, num) in page.iter().enumerate() {
				itoa::fmt(&mut query, *num)?;
				if i != page.len() - 1 {
					query.push(',');
				}
			}
			query.push_str(") ORDER BY block_num");
			let blocks = sqlx::query_as(query.as_str()).fetch_all(&mut *conn).await?;
			yield blocks;
		}
	})
}

/// Get up to `max_block_load` extrinsics which are not present in the `extrinsics` table.
/// Ordered from least to greatest number.
pub(crate) async fn blocks_missing_extrinsics(
	conn: &mut PgConnection,
	max_block_load: u32,
) -> Result<Vec<(u32, Vec<u8>, Vec<u8>, u32)>> {
	let blocks = sqlx::query_as!(
		BlockExtrinsics,
		"
		SELECT block_num, hash, ext, spec FROM blocks
		WHERE NOT EXISTS
			(SELECT number FROM extrinsics WHERE extrinsics.number = blocks.block_num)
		AND block_num > (SELECT MAX(number) FROM extrinsics)
		ORDER BY block_num ASC
		LIMIT $1
		",
		i64::from(max_block_load)
	)
	.fetch_all(conn)
	.await?
	.into_iter()
	.map(|b| (b.block_num as u32, b.hash, b.ext, b.spec as u32))
	.collect();

	Ok(blocks)
}

/// Get upgrade blocks starting from a spec.
/// Will always return one previous to `from`.
/// So if you want upgrade specs `from` 30 for polkadot,
/// this function will also return spec/block_num 29.
pub(crate) async fn upgrade_blocks_from_spec(conn: &mut sqlx::PgConnection, from: u32) -> Result<HashMap<u32, u32>> {
	let from = i32::try_from(from)?;
	let blocks = sqlx::query_as!(
		BlockNumSpec,
		r#"
			SELECT DISTINCT ON (spec) spec, block_num
			FROM blocks
			WHERE spec != 0
			ORDER BY spec, block_num ASC
		"#,
	)
	.fetch_all(conn)
	.await?
	.into_iter()
	.tuple_windows()
	.filter(|(_curr, next)| next.spec >= from)
	.map(|(curr, _)| (curr.block_num as u32, curr.spec as u32))
	.collect::<HashMap<u32, u32>>();

	Ok(blocks)
}

pub async fn past_and_present_version(
	conn: &mut PgConnection,
	spec: i32,
) -> Result<(Option<u32>, u32, Option<Vec<u8>>, Vec<u8>)> {
	let version = sqlx::query_as!(
		PastAndPresentVersion,
		"
	SELECT version as present, past_version as past, meta as metadata, past_metadata FROM (
		SELECT
			version, meta,
			LAG(version, 1) OVER (ORDER BY version) as past_version,
			LAG(meta, 1) OVER (ORDER BY version) as past_metadata
		FROM metadata
	) as z WHERE version = $1;
	",
		spec
	)
	.fetch_one(conn)
	.await
	.map(|v| (v.past.map(|v| v as u32), v.present as u32, v.past_metadata, v.metadata))?;

	Ok(version)
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
	};
	use anyhow::Error;
	use async_std::task;
	use futures::StreamExt;
	use sp_api::{BlockT, HeaderT};
	use sp_storage::StorageKey;
	use sqlx::{pool::PoolConnection, postgres::Postgres};
	use test_common::TestGuard;

	use polkadot_service::{Block, Hash};

	// kusama block height dataset starts at this number
	const BLOCK_START: usize = 3_000_000;

	// Setup a data scheme such that:
	// - blocks 0 - 200 will be missing from the `storage` table
	// - blocks 0 - 400 will be missing from the `extrinsics` table
	// - all blocks will be present in the `blocks` table
	async fn setup_data_scheme() -> Result<PoolConnection<Postgres>, Error> {
		let mock_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

		let blocks: Vec<BlockModel> = test_common::get_kusama_blocks()?.drain(0..1000).map(BlockModel::from).collect();
		let blocks = BlockModelDecoder::<Block>::with_vec(blocks)?;

		let database = Database::new(&test_common::DATABASE_URL.to_string()).await?;
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

		Ok(database.conn().await?)
	}

	#[test]
	fn should_get_missing_storage() -> Result<(), Error> {
		crate::initialize();
		let _guard = TestGuard::lock();
		let mut conn = task::block_on(setup_data_scheme())?;
		let items = task::block_on(missing_storage_blocks(&mut conn))?;

		assert_eq!(items.len(), 200);
		assert_eq!(items.iter().min(), Some(&3_000_001u32));
		assert_eq!(items.iter().max(), Some(&3_000_200u32));
		Ok(())
	}

	#[test]
	fn should_paginate_blocks() -> Result<(), Error> {
		crate::initialize();
		let _guard = TestGuard::lock();
		task::block_on(async {
			let mut conn = setup_data_scheme().await?;
			let mut block_nums: Vec<u32> =
				vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21];
			block_nums.iter_mut().for_each(|b| {
				*b += BLOCK_START as u32;
			});
			let mut pages = blocks_paginated(&mut conn, block_nums.as_slice(), 7)
				.map(|b| (b.unwrap().into_iter().map(|b| b.block_num - BLOCK_START as i32)).collect::<Vec<_>>());

			assert_eq!(vec![1, 2, 3, 4, 5, 6, 7], pages.next().await.unwrap());
			assert_eq!(vec![8, 9, 10, 11, 12, 13, 14], pages.next().await.unwrap());
			assert_eq!(vec![15, 16, 17, 18, 19, 20, 21], pages.next().await.unwrap());
			Ok(())
		})
	}
}
