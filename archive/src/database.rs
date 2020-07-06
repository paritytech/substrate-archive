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

mod batch;
pub mod models;
mod prepare_sql;

use async_trait::async_trait;
use codec::Encode;
use sp_runtime::traits::{Block as BlockT, Header as _, NumberFor};
use sqlx::{PgConnection, Postgres};

use self::{
    models::*,
    prepare_sql::{BindAll, PrepareBatchSql as _, PrepareSql as _},
};
use crate::{
    error::{ArchiveResult, Error as ArchiveError},
    types::*,
};

pub type DbReturn = Result<u64, ArchiveError>;
pub type DbConn = sqlx::Pool<PgConnection>;

#[async_trait]
pub trait Insert: Sync {
    async fn insert(mut self, db: DbConn) -> DbReturn
    where
        Self: Sized;
}
pub struct Database {
    /// pool of database connections
    pool: DbConn,
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
    pub fn new(pool: &DbConn) -> Self {
        Self { pool: pool.clone() }
    }

    pub fn pool(&self) -> &sqlx::Pool<PgConnection> {
        &self.pool
    }

    pub async fn insert(&self, data: impl Insert) -> ArchiveResult<u64> {
        data.insert(self.pool.clone()).await
    }
}

#[async_trait]
impl<B> Insert for Block<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn insert(mut self, db: DbConn) -> DbReturn {
        log::trace!(
            "block_num = {:?}, hash = {:X?}",
            self.inner.block.header().number(),
            hex::encode(self.inner.block.header().hash().as_ref())
        );
        self.single_insert()?.execute(&db).await.map_err(Into::into)
    }
}

impl<'a, B> BindAll<'a> for Block<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        let parent_hash = self.inner.block.header().parent_hash().as_ref();
        let hash = self.inner.block.header().hash();
        let block_num: u32 = (*self.inner.block.header().number()).into();
        let state_root = self.inner.block.header().state_root().as_ref();
        let extrinsics_root = self.inner.block.header().extrinsics_root().as_ref();
        let digest = self.inner.block.header().digest().encode();
        let extrinsics = self.inner.block.extrinsics().encode();

        Ok(query
            .bind(parent_hash)
            .bind(hash.as_ref())
            .bind(block_num)
            .bind(state_root)
            .bind(extrinsics_root)
            .bind(digest.as_slice())
            .bind(extrinsics.as_slice())
            .bind(self.spec))
    }
}

#[async_trait]
impl<B: BlockT> Insert for StorageModel<B> {
    async fn insert(mut self, db: DbConn) -> DbReturn {
        self.single_insert()?.execute(&db).await.map_err(Into::into)
    }
}

#[async_trait]
impl<B: BlockT> Insert for Vec<StorageModel<B>> {
    async fn insert(mut self, db: DbConn) -> DbReturn {
        let chunks = self.chunks(2048);
        let mut rows_changed = 0;
        for s in chunks {
            let storg = s.to_vec();
            let sql = self.build_sql(Some(storg.len() as u32));
            rows_changed += storg
                .batch_insert(sql.as_str(), |q| q.execute(&db))?
                .await?;
        }
        Ok(rows_changed)
    }
}

impl<'a, B: BlockT> BindAll<'a> for StorageModel<B> {
    fn bind_all_arguments(
        &self,
        query: sqlx::Query<'a, Postgres>,
    ) -> ArchiveResult<sqlx::Query<'a, Postgres>> {
        Ok(query
            .bind(self.block_num())
            .bind(self.hash().as_ref())
            .bind(self.is_full())
            .bind(self.key().0.as_slice())
            .bind(self.data().map(|d| d.0.as_slice())))
    }
}

#[async_trait]
impl Insert for Metadata {
    async fn insert(mut self, db: DbConn) -> DbReturn {
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
impl<B> Insert for BatchBlock<B>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn insert(mut self, db: DbConn) -> DbReturn {
        log::trace!("Batch inserting {} blocks into DB", self.inner().len());
        let mut rows_changed = 0;
        for blocks in self.inner().chunks(2048) {
            let blocks = blocks.to_vec();
            let sql = blocks.build_sql(None);
            rows_changed += blocks
                .batch_insert(sql.as_str(), |q| q.execute(&db))?
                .await?;
        }
        Ok(rows_changed)
    }
}

#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;
}
