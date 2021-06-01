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

//! IO for the PostgreSQL database connected to Substrate Archive Node
//! Handles inserting of data into the database

mod batch;
pub mod listener;
pub mod models;
pub mod queries;

use std::{
	convert::{TryFrom, TryInto},
	fmt,
	time::Duration,
};

use codec::Encode;
use serde::Deserialize;
use sqlx::{
	pool::PoolConnection,
	postgres::{PgConnection, PgPool, PgPoolOptions, Postgres},
	Connection,
};

use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};

use self::batch::Batch;
pub use self::{listener::*, models::*};
use crate::{
	error::{ArchiveError, Result},
	types::*,
	wasm_tracing::Traces,
};

/// Run all the migrations.
pub async fn migrate<T: AsRef<str>>(url: T) -> Result<()> {
	let mut conn = PgConnection::connect(url.as_ref()).await?;
	sqlx::migrate!("./src/migrations/").run(&mut conn).await?;
	Ok(())
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DatabaseConfig {
	/// PostgreSQL url.
	pub url: String,
}

impl fmt::Display for DatabaseConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.url)
	}
}

#[derive(Clone)]
pub struct Database {
	/// pool of database connections
	pool: PgPool,
	url: String,
}

impl Database {
	/// Connect to the database
	pub async fn new(url: String) -> Result<Self> {
		let pool = PgPoolOptions::new()
			.min_connections(4)
			.max_connections(28)
			.idle_timeout(Duration::from_millis(3600)) // kill connections after 3.6 seconds of idle
			.connect(url.as_str())
			.await?;
		Ok(Self { pool, url })
	}

	/// Start the database with a pre-defined pool
	#[allow(unused)]
	pub fn with_pool(url: String, pool: PgPool) -> Self {
		Self { pool, url }
	}

	#[allow(unused)]
	pub async fn insert(&self, data: impl Insert) -> Result<u64> {
		let mut conn = self.pool.acquire().await?;
		let res = data.insert(&mut conn).await?;
		Ok(res)
	}

	pub async fn conn(&self) -> Result<DbConn> {
		self.pool.acquire().await.map_err(Into::into)
	}

	pub fn pool(&self) -> &sqlx::PgPool {
		&self.pool
	}
}

pub type DbReturn = Result<u64>;
pub type DbConn = PoolConnection<Postgres>;

#[async_trait::async_trait]
pub trait Insert: Send + Sized {
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn;
}

#[async_trait::async_trait]
impl<B> Insert for Block<B>
where
	B: BlockT,
	NumberFor<B>: Into<u32>,
{
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		log::info!("Inserting single block");
		log::trace!(
			"block_num = {:?}, hash = {:X?}",
			self.inner.block.header().number(),
			hex::encode(self.inner.block.header().hash().as_ref())
		);
		let query = sqlx::query(
			r#"
            INSERT INTO blocks (parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec) VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING
        "#,
		);
		let parent_hash = self.inner.block.header().parent_hash().as_ref();
		let hash = self.inner.block.header().hash();
		let block_num: u32 = (*self.inner.block.header().number()).into();
		let state_root = self.inner.block.header().state_root().as_ref();
		let extrinsics_root = self.inner.block.header().extrinsics_root().as_ref();
		let digest = self.inner.block.header().digest().encode();
		let extrinsics = self.inner.block.extrinsics().encode();

		query
			.bind(parent_hash)
			.bind(hash.as_ref())
			.bind(block_num)
			.bind(state_root)
			.bind(extrinsics_root)
			.bind(digest.as_slice())
			.bind(extrinsics.as_slice())
			.bind(self.spec)
			.execute(conn)
			.await
			.map(|d| d.rows_affected())
			.map_err(Into::into)
	}
}

#[async_trait::async_trait]
impl<B> Insert for BatchBlock<B>
where
	B: BlockT,
	NumberFor<B>: Into<u32>,
{
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		let mut batch = Batch::new(
			"blocks",
			r#"
            INSERT INTO "blocks" (
                parent_hash, hash, block_num, state_root, extrinsics_root, digest, ext, spec
            ) VALUES
            "#,
			r#"
            ON CONFLICT DO NOTHING
            "#,
		);
		for b in self.inner {
			batch.reserve(8)?;
			if batch.current_num_arguments() > 0 {
				batch.append(",");
			}
			let parent_hash = b.inner.block.header().parent_hash().as_ref();
			let hash = b.inner.block.header().hash();
			let block_num: u32 = (*b.inner.block.header().number()).into();
			let state_root = b.inner.block.header().state_root().as_ref();
			let extrinsics_root = b.inner.block.header().extrinsics_root().as_ref();
			let digest = b.inner.block.header().digest().encode();
			let extrinsics = b.inner.block.extrinsics().encode();
			batch.append("(");
			batch.bind(parent_hash)?;
			batch.append(",");
			batch.bind(hash.as_ref())?;
			batch.append(",");
			batch.bind(block_num)?;
			batch.append(",");
			batch.bind(state_root)?;
			batch.append(",");
			batch.bind(extrinsics_root)?;
			batch.append(",");
			batch.bind(digest.as_slice())?;
			batch.append(",");
			batch.bind(extrinsics.as_slice())?;
			batch.append(",");
			batch.bind(b.spec)?;
			batch.append(")");
		}
		Ok(batch.execute(conn).await?)
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Insert for StorageModel<B> {
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		log::info!("Inserting Single Storage");
		sqlx::query(
			r#"
                INSERT INTO storage (
                    block_num, hash, is_full, key, storage
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
                    hash = EXCLUDED.hash,
                    key = EXCLUDED.key,
                    storage = EXCLUDED.storage,
                    is_full = EXCLUDED.is_full
            "#,
		)
		.bind(self.block_num())
		.bind(self.hash().as_ref())
		.bind(self.is_full())
		.bind(self.key().0.as_slice())
		.bind(self.data().map(|d| d.0.as_slice()))
		.execute(conn)
		.await
		.map(|d| d.rows_affected())
		.map_err(Into::into)
	}
}

#[async_trait::async_trait]
impl<B: BlockT> Insert for Vec<StorageModel<B>> {
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		let mut batch = Batch::new(
			"storage",
			r#"
            INSERT INTO "storage" (
                block_num, hash, is_full, key, storage
            ) VALUES
            "#,
			r#"
            ON CONFLICT (hash, key, md5(storage)) DO UPDATE SET
                hash = EXCLUDED.hash,
                key = EXCLUDED.key,
                storage = EXCLUDED.storage,
                is_full = EXCLUDED.is_full
            "#,
		);

		for s in self {
			batch.reserve(5)?;
			if batch.current_num_arguments() > 0 {
				batch.append(",");
			}
			batch.append("(");
			batch.bind(s.block_num())?;
			batch.append(",");
			batch.bind(s.hash().as_ref())?;
			batch.append(",");
			batch.bind(s.is_full())?;
			batch.append(",");
			batch.bind(s.key().0.as_slice())?;
			batch.append(",");
			batch.bind(s.data().map(|d| d.0.as_slice()))?;
			batch.append(")");
		}
		Ok(batch.execute(conn).await?)
	}
}

#[async_trait::async_trait]
impl Insert for Metadata {
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		log::debug!("Inserting Metadata, version = {}", self.version());
		sqlx::query(
			r#"
            INSERT INTO metadata (version, meta)
            VALUES($1, $2)
            ON CONFLICT DO NOTHING
        "#,
		)
		.bind(self.version())
		.bind(self.meta())
		.execute(conn)
		.await
		.map(|d| d.rows_affected())
		.map_err(Into::into)
	}
}

#[async_trait::async_trait]
impl Insert for Traces {
	async fn insert(mut self, conn: &mut DbConn) -> DbReturn {
		log::debug!("Inserting Trace Data");
		let mut batch = Batch::new(
			"state_tracing",
			r#"
			INSERT INTO "state_traces" (
				block_num, hash, is_event, timestamp, duration, file, line, trace_id, trace_parent_id, target, name, traces
			) VALUES
			"#,
			r#"
			ON CONFLICT DO NOTHING
			"#,
		);

		for span in self.spans.iter() {
			let id = i32::try_from(span.id.into_u64())?;
			let parent_id: Option<i32> =
				if let Some(id) = &span.parent_id { Some(i32::try_from(id.into_u64())?) } else { None };
			let overall_time: i64 = time_to_std(span.overall_time)?.as_nanos().try_into()?;
			batch.reserve(12)?;
			if batch.current_num_arguments() > 0 {
				batch.append(",");
			}
			batch.append("(");
			batch.bind(self.block_num())?; // block_numk
			batch.append(",");
			batch.bind(self.hash())?; // hash
			batch.append(",");
			batch.bind(false)?; // is_event
			batch.append(",");
			batch.bind(span.start_time)?; // timestamp
			batch.append(",");
			batch.bind(overall_time)?; // duration
			batch.append(",");
			batch.bind(span.file.as_ref())?; // file
			batch.append(",");
			batch.bind(span.line)?; // line
			batch.append(",");
			batch.bind(id)?; // trace_id
			batch.append(",");
			batch.bind(parent_id)?; // trace_parent_id
			batch.append(",");
			batch.bind(&span.target)?; // target
			batch.append(",");
			batch.bind(&span.name)?; // name
			batch.append(",");
			batch.bind(sqlx::types::Json(&span.values))?; // traces
			batch.append(")");
		}

		for event in self.events.iter() {
			let parent_id = event.parent_id.as_ref().map(|id| i32::try_from(id.into_u64())).transpose()?;
			batch.reserve(12)?;
			if batch.current_num_arguments() > 0 {
				batch.append(",");
			}
			batch.append("(");
			batch.bind(self.block_num())?; // block number
			batch.append(",");
			batch.bind(self.hash())?; // hash
			batch.append(",");
			batch.bind(true)?; // is_event
			batch.append(",");
			batch.bind(event.time)?; //time
			batch.append(",");
			batch.bind(Option::<i64>::None)?; // an event won't have a duration
			batch.append(",");
			batch.bind(&event.file)?; // file
			batch.append(",");
			batch.bind(event.line)?; // line
			batch.append(",");
			batch.bind(Option::<i32>::None)?; // Event has no ID
			batch.append(",");
			batch.bind(parent_id)?; // parent ikd
			batch.append(",");
			batch.bind(&event.target)?; // target
			batch.append(",");
			batch.bind(&event.name)?; // name
			batch.append(",");
			batch.bind(sqlx::types::Json(&event.values))?; // values
			batch.append(")");
		}

		Ok(batch.execute(conn).await?)
	}
}

// Chrono depends on an error type in `time` that is a full version behind the one that SQLX uses
// This function avoids depending on two time lib.
// Old time is disabled in chrono by not providing the feature flag in Cargo.toml.
fn time_to_std(time: chrono::Duration) -> Result<Duration> {
	time.to_std().map_err(|_| ArchiveError::TimestampOutOfRange)
}
