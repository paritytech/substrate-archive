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
use sp_runtime::traits::Header as _;
use sqlx::PgConnection;
use std::{convert::TryFrom, env, sync::RwLock};

use desub::decoder::{GenericExtrinsic, GenericSignature, Metadata};
use subxt::system::System;

use self::prepare_sql::PrepareSql as _;
use crate::{error::Error as ArchiveError, queries, types::Substrate, types::*};

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
    pub fn new(pool: &DbConnection) -> Result<Self, ArchiveError> {
        Ok(Self { pool: pool.clone() })
    }

    pub async fn insert(&self, data: impl Insert) -> Result<u64, ArchiveError> {
        data.insert(self.pool.clone()).await
    }
}

// TODO Make storage insertions generic over any type of insertin
// not only timestamps
#[async_trait]
impl<T> Insert for Storage<T>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        unimplemented!();
    }
}

#[async_trait]
impl<T> Insert for BatchStorage<T>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        unimplemented!();
    }
}

#[async_trait]
impl<T> Insert for Block<T>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        log::info!("hash = {:X?}", self.inner.block.header.hash().as_ref());
        log::info!("block_num = {:?}", self.inner.block.header.number());

        self.prep_insert()?.execute(&db).await.map_err(Into::into)
    }
}

#[async_trait]
impl<T> Insert for Vec<Extrinsic<T>>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        let mut query = None;
        for e in self.into_iter() {
            if query.is_some() {
                query = Some(e.add(query.expect("Checked for existence; qed"))?)
            } else {
                query = Some(e.prep_insert()?)
            }
        }
        query.unwrap().execute(&db).await.map_err(Into::into)
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

        let mut query = None;
        for block in self.inner().into_iter() {
            if query.is_some() {
                query = Some(block.add(query.expect("Checked for existence; qed"))?)
            } else {
                query = Some(block.prep_insert()?)
            }
        }
        query
            .expect("Query should not be none")
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
