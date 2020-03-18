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

pub mod db_middleware;
pub mod models;
pub mod schema;

use async_trait::async_trait;
use codec::Decode;
use diesel::{pg::PgConnection, prelude::*, sql_types::BigInt};
use dotenv::dotenv;
use log::*;
use runtime_primitives::traits::Header;

use frame_system::Trait as System;

use std::{convert::TryFrom, env};

use crate::{
    database::{
        db_middleware::AsyncDiesel,
        models::{InsertBlock, InsertBlockOwned},
        schema::{blocks, inherents, signed_extrinsics},
    },
    error::Error as ArchiveError,
    queries,
    types::{BatchBlock, BatchStorage, Block, Data, Storage},
};

pub type DbReturn = Result<(), ArchiveError>;

#[async_trait]
pub trait Insert: Sync {
    async fn insert(self, db: AsyncDiesel<PgConnection>) -> DbReturn
    where
        Self: Sized;
}

#[async_trait]
impl<T> Insert for Data<T>
where
    T: System,
{
    async fn insert(self, db: AsyncDiesel<PgConnection>) -> DbReturn {
        match self {
            Data::Block(block) => block.insert(db).await,
            Data::Storage(storage) => storage.insert(db).await,
            Data::BatchBlock(blocks) => blocks.insert(db).await,
            Data::BatchStorage(storage) => storage.insert(db).await,
            o => Err(ArchiveError::UnhandledDataType(format!("{:?}", o))),
        }
    }
}

/// Database object which communicates with Diesel in a (psuedo)asyncronous way
/// via `AsyncDiesel`
pub struct Database {
    db: AsyncDiesel<PgConnection>,
}

impl Database {
    /// Connect to the database
    pub fn new() -> Result<Self, ArchiveError> {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL")?;
        let db = AsyncDiesel::new(&database_url)?;
        Ok(Self { db })
    }

    pub async fn insert(&self, data: impl Insert) -> Result<(), ArchiveError> {
        data.insert(self.db.clone()).await
    }

    pub async fn query_missing_blocks(
        &self,
        latest: Option<u64>,
    ) -> Result<Vec<u64>, ArchiveError> {
        #[derive(QueryableByName, PartialEq, Debug)]
        pub struct Blocks {
            #[column_name = "generate_series"]
            #[sql_type = "BigInt"]
            block_num: i64,
        };

        self.db
            .run(move |conn| {
                let blocks: Vec<Blocks> = queries::missing_blocks(latest).load(&conn)?;
                Ok(blocks
                    .iter()
                    .map(|b| {
                        u64::try_from(b.block_num)
                            .expect("Block number should never be negative; qed")
                    })
                    .collect::<Vec<u64>>())
            })
            .await
    }
}

// TODO Make storage insertions generic over any type of insertin
// not only timestamps
#[async_trait]
impl<T> Insert for Storage<T>
where
    T: System,
{
    async fn insert(self, db: AsyncDiesel<PgConnection>) -> DbReturn {
        // use self::schema::blocks::dsl::{blocks, hash, time};
        let date_time = self.get_timestamp()?;
        let hsh = self.hash().clone();
        db.run(move |conn| {
            trace!("inserting timestamp for: {}", hsh);
            let len = 1;
            // WARN this just inserts a timestamp
            /*
            diesel::update(blocks.filter(hash.eq(hsh.as_ref())))
                .set(time.eq(Some(&date_time)))
                .execute(&conn)
                .map_err(|e| ArchiveError::from(e))?;
             */
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl<T> Insert for BatchStorage<T>
where
    T: System,
{
    async fn insert(self, db: AsyncDiesel<PgConnection>) -> DbReturn {
        use self::schema::blocks::dsl::{blocks, hash, time};
        debug!("Inserting {} items via Batch Storage", self.inner().len());
        let storage: Vec<Storage<T>> = self.consume();
        db.run(move |conn| {
            let len = storage.len();
            for item in storage.into_iter() {
                let date_time = item.get_timestamp()?;
                /*
                diesel::update(blocks.filter(hash.eq(item.hash().as_ref())))
                    .set(time.eq(Some(&date_time)))
                    .execute(&conn)?;
                 */
            }
            info!("Done inserting storage!");
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl<T> Insert for Block<T>
where
    T: System,
{
    async fn insert(self, db: AsyncDiesel<PgConnection>) -> DbReturn {
        let block = self.inner().block.clone();
        info!("HASH: {:X?}", block.header.hash().as_ref());
        info!("Block Num: {:?}", block.header.number());
        let extrinsics = DbExtrinsic::decode::<T>(&block.extrinsics, &block.header)?;
        // TODO Optimize
        db.run(move |conn| {
            diesel::insert_into(blocks::table)
                .values(InsertBlock {
                    parent_hash: block.header.parent_hash().as_ref(),
                    hash: block.header.hash().as_ref(),
                    block_num: &((*block.header.number()).into() as i64),
                    state_root: block.header.state_root().as_ref(),
                    extrinsics_root: block.header.extrinsics_root().as_ref(),
                    time: extrinsics.extra().time().as_ref(),
                })
                .on_conflict(blocks::hash)
                .do_nothing()
                .execute(&conn)?;

            let (mut signed_ext, mut unsigned_ext) = (Vec::new(), Vec::new());
            let len = extrinsics.0.len() + 1; // 1 for the block
            for e in extrinsics.0.into_iter() {
                match e {
                    DbExtrinsic::Signed(e) => signed_ext.push(e),
                    DbExtrinsic::NotSigned(e, _) => unsigned_ext.push(e),
                }
            }

            diesel::insert_into(inherents::table)
                .values(unsigned_ext)
                .execute(&conn)?;

            diesel::insert_into(signed_extrinsics::table)
                .values(signed_ext)
                .execute(&conn)?;

            Ok(())
        })
        .await
    }
}

#[async_trait]
impl<T> Insert for BatchBlock<T>
where
    T: System,
{
    async fn insert(self, db: AsyncDiesel<PgConnection>) -> DbReturn {
        let mut extrinsics: Extrinsics = Extrinsics(Vec::new());
        info!("Batch inserting {} blocks into DB", self.inner().len());
        let blocks = self
            .inner()
            .iter()
            .map(|block| {
                let block = block.block.clone();
                let mut block_ext: Extrinsics =
                    DbExtrinsic::decode::<T>(&block.extrinsics, &block.header)?;
                debug!("Block Ext: {:?}", block_ext);

                let block = InsertBlockOwned {
                    parent_hash: block.header.parent_hash().as_ref().to_vec(),
                    hash: block.header.hash().as_ref().to_vec(),
                    block_num: (*block.header.number()).into() as i64,
                    state_root: block.header.state_root().as_ref().to_vec(),
                    extrinsics_root: block.header.extrinsics_root().as_ref().to_vec(),
                    time: block_ext.extra().time(),
                };
                extrinsics.0.append(&mut block_ext.0);
                Ok(block)
            })
            // .filter_map(|b: Result<_, ArchiveError>| b.ok())
            .collect::<Result<Vec<InsertBlockOwned>, ArchiveError>>()?;

        let (mut signed_ext, mut unsigned_ext) = (Vec::new(), Vec::new());

        for e in extrinsics.0.into_iter() {
            match e {
                DbExtrinsic::Signed(v) => signed_ext.push(v),
                DbExtrinsic::NotSigned(v, _) => unsigned_ext.push(v),
            }
        }

        // batch insert everything we've formatted/collected into the database 10,000 items at a time
        db.run(move |conn| {
            let len = blocks.len() + unsigned_ext.len() + signed_ext.len();
            for chunks in blocks.as_slice().chunks(10_000) {
                info!("{} blocks to insert", chunks.len());
                diesel::insert_into(blocks::table)
                    .values(chunks)
                    .on_conflict(blocks::hash)
                    .do_nothing()
                    .execute(&conn)?;
            }
            for chunks in unsigned_ext.as_slice().chunks(2_500) {
                info!("{} unsigned extrinsics to insert", chunks.len());
                diesel::insert_into(inherents::table)
                    .values(chunks)
                    .execute(&conn)?;
            }
            for chunks in signed_ext.as_slice().chunks(2_500) {
                info!("{} signed extrinsics to insert", chunks.len());
                diesel::insert_into(signed_extrinsics::table)
                    .values(chunks)
                    .execute(&conn)?;
            }
            info!("Done {} Inserting Blocks and Extrinsics", len);
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;
}
