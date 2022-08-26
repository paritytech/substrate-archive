// Copyright 2018-2021 Parity Technologies (UK) Ltd.
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

//! A method of dynamic queries with SQLx
//! Taken from this Gist by @mehcode (Github): https://gist.github.com/mehcode/c476922be0290a4f8502d18701cc8c74
//! This is sort of temporary until SQLx develops their dynamic query builder: https://github.com/launchbadge/sqlx/issues/291
//! and `Quaint` switches to SQLx as a backend: https://github.com/prisma/quaint/issues/138

use async_std::{future::timeout, task};
use futures::{future, stream, StreamExt, TryFutureExt, TryStreamExt};
use sqlx::{
	encode::Encode,
	postgres::{PgArguments, PgConnection, PgPool, Postgres},
	prelude::*,
	Arguments, Executor,
};

use std::{
	convert::TryInto,
	sync::atomic::{AtomicUsize, Ordering},
	time::Duration,
};

use crate::error::{ArchiveError, Result};

// CHUNK_MAX is mostly picked from my own testing.
// It might be worth making it configurable from the TOML,
// however for the general case I think it is OK to give a hard-coded value, at least for now.

// CHUNK_MAX was chosen by running archive and checking how long it took to insert storage into Postgres.
// Insertion time is somewhat dependant on this value, especially on machines with many cores available and/or
// on machines with drives allowing concurrent read/writes. Although on machines with less cores/sequential drives
// this value matters less anyway.
// Insertion times were a big hangup in previous iterations, causing tasks to time out.
// (~100-200 blocks to insert, each with 30-400 changes each,
// 400 being on the extreme end with a block that includes some more intensive extrinsics).

// This was fixed by lowering CHUNK_MAX, but allowing storage to be inserted concurrently based on how many free idle
// connections to Postgres exist. This way instead of one query taking ~10-20s to insert 30K items,
// we can have 3-4 or more which each take less time, based on how many threads exist on the system.
// Insertion times for blocks have never really been an issue, so this is mostly an optimization for storage/traces
const CHUNK_MAX: usize = 5_000;

pub struct Chunk {
	query: String,
	pub arguments: PgArguments,

	// FIXME: Would be nice if PgArguments exposed the # of args as `.len()`
	pub args_len: usize,
}

impl Chunk {
	fn new(sql: &str) -> Self {
		let mut query = String::with_capacity(1024 * 8);
		query.push_str(sql);

		Self { query, arguments: PgArguments::default(), args_len: 0 }
	}

	pub fn append(&mut self, sql: &str) {
		self.query.push_str(sql);
	}

	pub fn bind<'a, T: 'a>(&mut self, value: T) -> Result<()>
	where
		T: Encode<'a, Postgres> + Type<Postgres> + Send,
	{
		self.arguments.add(value);
		self.query.push('$');
		itoa::fmt(&mut self.query, self.args_len + 1)?;
		self.args_len += 1;

		Ok(())
	}

	async fn execute<'a, E>(self, conn: E) -> Result<u64>
	where
		E: Executor<'a, Database = Postgres>,
	{
		let done = sqlx::query_with(&*self.query, self.arguments.into_arguments()).execute(conn).await?;
		Ok(done.rows_affected())
	}
}

pub struct Batch {
	#[allow(unused)]
	name: &'static str,
	leading: String,
	trailing: String,
	#[allow(clippy::type_complexity)]
	with: Option<Box<dyn Fn(&mut Chunk) -> Result<()> + Send>>,
	chunks: Vec<Chunk>,
	index: usize,
	len: usize,
}

impl Batch {
	pub fn new(name: &'static str, leading: &str, trailing: &str) -> Self {
		Self {
			name,
			leading: leading.to_owned(),
			trailing: trailing.to_owned(),
			chunks: vec![Chunk::new(leading)],
			with: None,
			index: 0,
			len: 0,
		}
	}

	#[allow(unused)]
	pub fn new_with(
		name: &'static str,
		leading: &str,
		trailing: &str,
		with: impl Fn(&mut Chunk) -> Result<()> + Send + 'static,
	) -> Result<Self> {
		let mut chunk = Chunk::new(leading);
		with(&mut chunk)?;

		Ok(Self {
			name,
			leading: leading.to_owned(),
			trailing: trailing.to_owned(),
			with: Some(Box::new(with)),
			chunks: vec![chunk],
			index: 0,
			len: 0,
		})
	}

	// ensure there is enough room for N more arguments
	pub fn reserve(&mut self, arguments: usize) -> Result<()> {
		self.len += 1;

		if self.chunks[self.index].args_len + arguments > CHUNK_MAX {
			let mut chunk = Chunk::new(&self.leading);

			if let Some(with) = &self.with {
				with(&mut chunk)?;
			}

			self.chunks.push(chunk);
			self.index += 1;
		}

		Ok(())
	}

	pub fn append(&mut self, sql: &str) {
		self.chunks[self.index].append(sql);
	}

	pub fn bind<'a, T: 'a>(&mut self, value: T) -> Result<()>
	where
		T: Encode<'a, Postgres> + Type<Postgres> + Send,
	{
		self.chunks[self.index].bind(value)
	}

	pub async fn execute(self, conn: &mut PgConnection) -> Result<u64> {
		let mut rows_affected = 0;
		if self.len > 0 {
			for mut chunk in self.chunks {
				chunk.append(&self.trailing);
				let done = chunk.execute(&mut *conn).await?;
				rows_affected += done;
			}
		}

		Ok(rows_affected)
	}

	/// Execute a batch concurrently. If a batch size is greater than CHUNK_MAX,
	/// the query will be executed in multiple independent futures.
	/// The amount of futures is controlled with `limit`. A limit of `None`
	/// will limit this function to the number of idle database connections.
	pub async fn execute_concurrent(self, conn: PgPool, limit: Option<usize>) -> Result<u64> {
		let rows_affected = AtomicUsize::new(0);
		let trailing = self.trailing.clone();
		let num_idle = |_| async {
			let c = conn.clone();
			Some(timeout(Duration::from_millis(100), task::spawn_blocking(move || c.num_idle())).await).transpose()
		};
		if self.len > 0 {
			stream::iter(self.chunks)
				.map(Ok)
				.try_for_each_concurrent(
					future::ok::<_, ()>(limit).or_else(num_idle).await.unwrap_or(Some(2)),
					|mut chunk| async {
						chunk.append(&trailing);
						let done = chunk.execute(&conn.clone()).await?;
						rows_affected.fetch_add(done.try_into()?, Ordering::Relaxed);
						Ok::<(), ArchiveError>(())
					},
				)
				.await?;
		}
		Ok(rows_affected.into_inner().try_into()?)
	}

	pub fn current_num_arguments(&self) -> usize {
		self.chunks[self.index].args_len
	}
}
