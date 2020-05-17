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
use sqlx::{PgConnection, Postgres, arguments::Arguments as _, postgres::PgArguments};
use std::{convert::TryFrom, env, sync::RwLock};

use desub::decoder::{GenericExtrinsic, GenericSignature, Metadata};
use subxt::system::System;

use self::prepare_sql::{PrepareSql as _, PrepareBatchSql as _, GetArguments};
use crate::{error::{Error as ArchiveError, ArchiveResult}, queries, types::Substrate, types::*};

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

    pub async fn insert(&self, data: impl Insert) -> ArchiveResult<u64> {
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
        self.single_insert()?.execute(&db).await.map_err(Into::into)
    }
}

impl<T> GetArguments for Block<T>
where
    T: Substrate + Send + Sync,
<T as System>::BlockNumber: Into<u32>,
{
    fn get_arguments(&self) -> ArchiveResult<PgArguments> {
        let parent_hash = self.inner.block.header.parent_hash().as_ref();
        let hash = self.inner.block.header.hash();
        let block_num: u32 = (*self.inner.block.header.number()).into();
        let state_root = self.inner.block.header.state_root().as_ref();
        let extrinsics_root = self.inner.block.header.extrinsics_root().as_ref();


        let mut arguments = PgArguments::default();
        arguments.add(parent_hash);
        arguments.add(hash.as_ref());
        arguments.add(block_num);
        arguments.add(state_root);
        arguments.add(extrinsics_root);
        Ok(arguments)
    }
}


#[async_trait]
impl<T> Insert for Vec<SignedExtrinsic<T>>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        let sql = self.build_sql();
        self.batch_insert(&sql)?.execute(&db).await.map_err(Into::into)
    }
}

impl<T> GetArguments for SignedExtrinsic<T>
where
    T: Substrate + Send + Sync,
{
    fn get_arguments(&self) -> ArchiveResult<PgArguments> {
        // FIXME
        // workaround for serde not serializing u128 to value
        // and diesel only supporting serde_Json::Value for jsonb in postgres
        // u128 are used in balance transfers
        let parameters = serde_json::to_string(&self.args())?;
        let parameters: serde_json::Value = serde_json::from_str(&parameters)?;

        let (addr, sig, extra) = self
            .signature()
            .ok_or(ArchiveError::DataNotFound("Signature".to_string()))?
            .parts();

        let addr = serde_json::to_string(addr)?;
        let sig = serde_json::to_string(sig)?;
        let extra = serde_json::to_string(extra)?;

        let addr: serde_json::Value = serde_json::from_str(&addr)?;
        let sig: serde_json::Value = serde_json::from_str(&sig)?;
        let extra: serde_json::Value = serde_json::from_str(&extra)?;

        let mut arguments = PgArguments::default();
        arguments.add(self.hash().as_ref());
        arguments.add(self.block_num());
        arguments.add(addr);
        arguments.add(self.ext_module());
        arguments.add(self.ext_call());
        arguments.add(parameters);
        arguments.add(self.index() as u32);
        arguments.add(sig);
        arguments.add(Some(extra));
        arguments.add(0 as u32);
        Ok(arguments)
    }
}

#[async_trait]
impl<T> Insert for Vec<Inherent<T>>
where
    T: Substrate + Send + Sync,
{
    async fn insert(self, db: DbConnection) -> DbReturn {
        let sql = self.build_sql();
        self.batch_insert(&sql)?.execute(&db).await.map_err(Into::into)
    }
}

impl<T> GetArguments for Inherent<T>
where
    T: Substrate + Send + Sync,
{
    fn get_arguments(&self) -> ArchiveResult<PgArguments> {
        // FIXME
        // workaround for serde not serializing u128 to value
        // and sqlx only supporting serde_Json::Value for jsonb in postgres
        // u128 are used in balance transfers
        // Can write own Postgres Encoder for value with sqlx
        let parameters = serde_json::to_string(&self.args()).unwrap();
        let parameters: serde_json::Value = serde_json::from_str(&parameters).unwrap();

        let mut arguments = PgArguments::default();
        arguments.add(self.hash().as_ref());
        arguments.add(self.block_num());
        arguments.add(self.ext_module());
        arguments.add(self.ext_call());
        arguments.add(parameters);
        arguments.add(self.index() as u32);
        arguments.add(0 as u32);
        Ok(arguments)
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
        let sql = self.inner().build_sql();
        self.inner().batch_insert(&sql)?.execute(&db).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;
}
