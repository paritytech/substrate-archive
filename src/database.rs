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

pub mod models;
pub mod schema;
pub mod db_middleware;

use log::*;
use futures::future::{self, Future};
use diesel::{prelude::*, pg::PgConnection, sql_types::{BigInt, Bytea}} ;
use codec::Decode;
use runtime_primitives::traits::{Header, Hash};
use dotenv::dotenv;
// use runtime_support::dispatch::IsSubType;
use runtime_primitives::{
    traits::{Block as BlockTrait, Extrinsic as ExtrinsicTrait},
    OpaqueExtrinsic
};

use std::{
    env,
    convert::{TryFrom, TryInto}
};

use crate::{
    error::Error as ArchiveError,
    extrinsics::{Extrinsic, DbExtrinsic},
    types::{BasicExtrinsic, Data, System, Block, Storage, BatchBlock, BatchStorage, ExtractCall},
    database::{
        models::{InsertBlock, InsertBlockOwned, InsertInherentOwned, InsertTransactionOwned},
        schema::{blocks, inherents, signed_extrinsics},
        db_middleware::AsyncDiesel
    },
    queries
};

pub type DbFuture = Box<dyn Future<Item = (), Error = ArchiveError> + Send >;

pub trait Insert {
    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError>;
}

impl<T> Insert for Data<T> where T: System + 'static {
    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        match self {
            Data::Block(block) => {
                block.insert(db)
            },
            Data::Storage(storage) => {
                storage.insert(db)
            },
            Data::BatchBlock(blocks) => {
                blocks.insert(db)
            },
            Data::BatchStorage(storage) => {
                storage.insert(db)
            },
            o @ _=> {
                Err(ArchiveError::UnhandledDataType(format!("{:?}", o)))
            }
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

    pub fn insert(&self, data: impl Insert) -> DbFuture {
        match data.insert(self.db.clone()) {
            Ok(v) => v,
            Err(e) => Box::new(future::err(e)),
        }
    }

    pub fn query_missing_blocks(&self
    ) -> impl Future<Item = Vec<u64>, Error = ArchiveError>
    {
        #[derive(QueryableByName, PartialEq, Debug)]
        pub struct Blocks {
            #[column_name = "generate_series"]
            #[sql_type = "BigInt"]
            block_num: i64
        };

        self.db.run(move |conn| {
            let blocks: Vec<Blocks> = queries::missing_blocks().load(&conn)?;
            Ok(blocks
                .iter()
                .map(|b| u64::try_from(b.block_num).expect("Block number should never be negative; qed"))
               .collect::<Vec<u64>>()
            )
        })
    }

    pub fn query_missing_timestamps<T>(&self
    ) -> impl Future<Item = Vec<T::Hash>, Error = ArchiveError>
    where T: System
    {
        #[derive(QueryableByName, PartialEq, Debug)]
        pub struct Hashes {
            #[column_name = "hash"]
            #[sql_type = "Bytea"]
            pub hash: Vec<u8>
        };

        self.db.run(move |conn| {
            let blocks: Vec<Hashes> = queries::missing_timestamp().load(&conn)?;
            Ok(blocks
               .iter()
               .map(|b| {
                   let old_hash = b.hash.clone();
                   let hash: T::Hash = Decode::decode(&mut b.hash.as_slice())
                       .expect("Immediate Decoding/Encoding should be infallible");
                   assert!(hash.as_ref() == old_hash.as_slice());
                   hash
               })
               .collect::<Vec<T::Hash>>()
            )
        })
    }
}

// TODO Make storage insertions generic over any type of insertin
// not only timestamps
impl<T> Insert for Storage<T>
where
    T: System
{
    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        use self::schema::blocks::dsl::{blocks, hash, time};
        let date_time = self.get_timestamp()?;
        let hsh = self.hash().clone();
        let fut = db.run(move |conn| {
            trace!("inserting timestamp for: {}", hsh);
            diesel::update(blocks.filter(hash.eq(hsh.as_ref())))
                .set(time.eq(Some(&date_time)))
                .execute(&conn)
                .map_err(Into::into)
        }).map(|_| ());

        Ok(Box::new(fut))
    }
}

impl<T> Insert for BatchStorage<T>
where
    T: System
{
    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        use self::schema::blocks::dsl::{blocks, hash, time};
        debug!("Inserting {} items via Batch Storage", self.inner().len());
        let storage: Vec<Storage<T>> = self.consume();
        let fut = db.run(move |conn| {
            for item in storage.into_iter() {
                let date_time = item.get_timestamp()?;
                diesel::update(blocks.filter(hash.eq(item.hash().as_ref())))
                    .set(time.eq(Some(&date_time)))
                    .execute(&conn)
                    .map_err(|e| ArchiveError::from(e))?;
            }
            Ok(())
        }).map(|_| ());

        Ok(Box::new(fut))
    }
}

impl<T> Insert for Block<T> where T: System + 'static {

    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {

        let block = self.inner().block.clone();
        info!("HASH: {:X?}", block.header.hash().as_ref());
        info!("Block Num: {:?}", block.header.number());
        let extrinsics = get_extrinsics::<T>(block.extrinsics(), &block.header)?;
        // TODO Optimize
        let fut = db.run(move |conn| {
            diesel::insert_into(blocks::table)
                .values(InsertBlock {
                    parent_hash: block.header.parent_hash().as_ref(),
                    hash: block.header.hash().as_ref(),
                    block_num: &(*block.header.number()).into(),
                    state_root: block.header.state_root().as_ref(),
                    extrinsics_root: block.header.extrinsics_root().as_ref(),
                    time: None
                })
                .execute(&conn)
                .map_err(|e| ArchiveError::from(e))?;

            let (mut signed_ext, mut unsigned_ext) = (Vec::new(), Vec::new());
            for e in extrinsics.into_iter() {
                match e {
                    DbExtrinsic::Signed(e) => signed_ext.push(e),
                    DbExtrinsic::NotSigned(e) => unsigned_ext.push(e),
                }
            }

            diesel::insert_into(inherents::table)
                .values(unsigned_ext)
                .execute(&conn)
                .map_err(|e| ArchiveError::from(e))?;

            diesel::insert_into(signed_extrinsics::table)
                .values(signed_ext)
                .execute(&conn)
                .map_err(Into::into)

        }).map(|_| ());

        Ok(Box::new(fut))
    }
}

impl<T> Insert for BatchBlock<T>
where
    T: System,
{

    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {

        let mut extrinsics: Vec<DbExtrinsic> = Vec::new();
        info!("Batch inserting {} blocks into DB", self.inner().len());
        let blocks = self
            .inner()
            .iter()
            .map(|block| {
                let block = block.block.clone();
                let mut block_ext: Vec<DbExtrinsic>
                    = get_extrinsics::<T>(block.extrinsics(), &block.header)?;

                extrinsics.append(&mut block_ext);
                Ok(InsertBlockOwned {
                    parent_hash: block.header.parent_hash().as_ref().to_vec(),
                    hash: block.header.hash().as_ref().to_vec(),
                    block_num: (*block.header.number()).into(),
                    state_root: block.header.state_root().as_ref().to_vec(),
                    extrinsics_root: block.header.extrinsics_root().as_ref().to_vec(),
                    time: None
                })
            })
            .filter_map(|b: Result<_, ArchiveError>| b.ok())
            .collect::<Vec<InsertBlockOwned>>();

            let (mut signed_ext, mut unsigned_ext) = (Vec::new(), Vec::new());

            for e in extrinsics.into_iter() {
                match e {
                    DbExtrinsic::Signed(e) => signed_ext.push(e),
                    DbExtrinsic::NotSigned(e) => unsigned_ext.push(e),
                }
            }

        // batch insert everything we've formatted/collected into the database 10,000 items at a time
        let fut = db.run(move |conn| {
            for chunks in blocks.as_slice().chunks(10_000) {
                info!("{}", chunks.len());
                diesel::insert_into(blocks::table)
                    .values(chunks)
                    .execute(&conn)
                    .map_err(|e| ArchiveError::from(e))?;
            }
            for chunks in unsigned_ext.as_slice().chunks(2_500) {
                info!("inserting {} unsigned extrinsics", chunks.len());
                diesel::insert_into(inherents::table)
                    .values(chunks)
                    .execute(&conn)
                    .map_err(|e| ArchiveError::from(e))?;
            }
            for chunks in signed_ext.as_slice().chunks(2_500) {
                info!("inserting {} signed extrinsics", chunks.len());
                diesel::insert_into(signed_extrinsics::table)
                    .values(chunks)
                    .execute(&conn)
                    .map_err(|e| ArchiveError::from(e))?;
            }
            Ok(())
        }).map(|_| ());
        Ok(Box::new(fut))
    }
}

fn get_extrinsics<T>(
    extrinsics: &[OpaqueExtrinsic],
    header: &T::Header,
    // db: &AsyncDiesel<PgConnection>,
) -> Result<Vec<DbExtrinsic>, ArchiveError> where T: System {
    extrinsics
        .iter()
        // enumerate is used here to preserve order/index of extrinsics
        .enumerate()
        .map(|(idx, x)| {
            Ok((idx, Extrinsic::new(&x)?))
        })
        .collect::<Vec<Result<(usize, BasicExtrinsic<T>), ArchiveError>>>()
        .into_iter()
        // we don't want to skip over _all_ extrinsics if decoding one extrinsic does not work
        .filter_map(|x: Result<(usize, BasicExtrinsic<T>), _>| {
            match x {
                Ok(v) => {
                    let number = (*header.number()).into();
                    Some(v.1.database_format(v.0.try_into().unwrap(), header, number))
                },
                Err(e) => {
                    error!("{:?}", e);
                    None
                }
            }
        })
        .collect::<Result<Vec<DbExtrinsic>, ArchiveError>>()
}

#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;




}
