// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

mod prepare_sql;

use async_trait::async_trait;
use codec::{Decode, Encode};
use futures::future::{self, TryFutureExt};
use sp_runtime::traits::Header as _;
use sqlx::{arguments::Arguments as _, postgres::PgArguments, PgConnection, Postgres};
use std::{convert::TryFrom, env, sync::RwLock};

use subxt::system::System;

use self::prepare_sql::{BindAll, PrepareBatchSql as _, PrepareSql as _};
use crate::{
    error::{ArchiveResult, Error as ArchiveError},
    queries,
    types::*,
};

pub type DbReturn = Result<u64, ArchiveError>;
pub type DbConnection = sqlx::Pool<PgConnection>;

#[async_trait]
pub trait Insert: Sync {
    async fn insert(self, db: DbConnection) -> DbReturn
    where
        Self: Sized;
}
pub struct Database {
    /// pool of database connections
    pool: DbConnection,
}

// clones a database connection
impl Clone for Database {
    fn clone(&self) -> Self {
        Database {
            pool: self.pool.clone(),
        }
    }
}

impl Database {
    /// Connect to the database
    pub fn new(pool: &DbConnection) -> ArchiveResult<Self> {
        Ok(Self { pool: pool.clone() })
    }

    pub fn pool(&self) -> &sqlx::Pool<PgConnection> {
        &self.pool
    }

    pub async fn insert(&self, data: impl Insert) -> ArchiveResult<u64> {
        data.insert(self.pool.clone()).await
    }
}

#[async_trait]
impl<T> Insert for Block<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        log::info!("block_num = {:?}, hash = {:X?}",
                   self.inner.block.header.number(),
                   hex::encode(self.inner.block.header.hash().as_ref()));
        self.single_insert()?.execute(&db).await.map_err(Into::into)
    }
}

impl<'a, T> BindAll<'a> for Block<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let parent_hash = self.inner.block.header.parent_hash().as_ref();
        let hash = self.inner.block.header.hash();
        let block_num: u32 = (*self.inner.block.header.number()).into();
        let state_root = self.inner.block.header.state_root().as_ref();
        let extrinsics_root = self.inner.block.header.extrinsics_root().as_ref();

        Ok(query
            .bind(parent_hash)
            .bind(hash.as_ref())
            .bind(block_num)
            .bind(state_root)
            .bind(extrinsics_root)
            .bind(self.spec))
    }
}

#[async_trait]
impl<T> Insert for Storage<T>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        self.single_insert()?.execute(&db).await.map_err(Into::into)
    }
}

#[async_trait]
impl<T> Insert for Vec<Storage<T>>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        log::info!("Inserting {} storage entries", self.len());
        let mut sizes = Vec::new();
        let chunks = self.chunks(12_000);

        for chunk in chunks.clone() {
            // FIXME should not clone here
            sizes.push(chunk.len())
        }

        let queries = sizes
            .into_iter()
            .map(|s| self.build_sql(Some(s as u32)))
            .collect::<Vec<String>>();
        let mut counter = 0;
        let mut futures = Vec::new();
        for s in chunks {
            let storg = s.to_vec();
            futures.push(storg.batch_insert(&queries[counter])?.execute(&db));
            counter += 1;
        }
        let mut rows_changed = 0;
        future::join_all(futures)
            .await
            .iter()
            .for_each(|r| match r {
                Ok(v) => rows_changed += v,
                Err(e) => log::error!("{:?}", e),
            });
        Ok(rows_changed)
    }
}

impl<'a, T> BindAll<'a> for Storage<T>
where
    T: Substrate + Send + Sync,
{
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        Ok(query
            .bind(self.block_num())
            .bind(self.hash().as_ref())
            .bind(self.key().0.as_slice())
            .bind(self.data().0.as_slice()))
    }
}

#[async_trait]
impl Insert for Metadata {
    async fn insert(self, db: DbConnection) -> DbReturn {
        self.single_insert()?.execute(&db).await.map_err(Into::into)
    }
}

impl<'a> BindAll<'a> for Metadata {
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        Ok(query.bind(self.version()).bind(self.meta()))
    }
}

#[async_trait]
impl<T> Insert for Vec<Extrinsic<T>>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        let mut rows_changed = 0;
        for ext in self.chunks(15_000) {
            let ext = ext.to_vec();
            let sql = ext.build_sql(None);
            rows_changed += ext.batch_insert(&sql)?.execute(&db).await?;
        }
        Ok(rows_changed)
    }
}

impl<'a, T> BindAll<'a> for Extrinsic<T>
where
    T: Substrate + Send + Sync,
{
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        Ok(query
            .bind(self.hash.as_slice())
            .bind(self.spec)
            .bind(self.index)
            .bind(self.inner.as_slice()))
    }
}

#[async_trait]
impl<T> Insert for BatchBlock<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        log::info!("Batch inserting {} blocks into DB", self.inner().len());
        let sql = self.inner().build_sql(None);
        self.inner()
            .batch_insert(&sql)?
            .execute(&db)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;
}
