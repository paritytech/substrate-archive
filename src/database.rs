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
use codec::{Encode, Decode};
use runtime_primitives::traits::Header;
use dotenv::dotenv;
// use runtime_support::dispatch::IsSubType;
use runtime_primitives::{
    traits::{Block as BlockTrait, Extrinsic},
    OpaqueExtrinsic, generic::UncheckedExtrinsic
};

use std::{
    env,
    convert::TryFrom
};

use crate::{
    error::Error as ArchiveError,
    types::{Data, System, Block, Storage, BatchBlock, BatchStorage, BasicExtrinsic, ExtractCall},
    database::{
        models::{InsertBlock, InsertBlockOwned, InsertInherentOwned},
        schema::{blocks, inherents},
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
                   let hash = b.hash.encode();
                   let hash: T::Hash = Decode::decode(&mut hash.as_slice())
                       .expect("Immediate Decoding/Encoding should be infallible");
                   debug!("OLDHASH {:X?}", old_hash);
                   debug!("OLDHASH {:?}", hash);
                   // assert!(hash.as_ref() == old_hash.as_slice());
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
        use self::schema::blocks::dsl::{blocks, time};
        let date_time = self.get_timestamp()?;
        let hash = self.hash().clone();
        let fut = db.run(move |conn| {
            diesel::update(blocks.find(hash.as_ref()))
                .set(time.eq(Some(&date_time)))
                .execute(&conn)
                .map_err(|e| e.into())
        }).map(|_| ());
        Ok(Box::new(fut))
    }
}

impl<T> Insert for BatchStorage<T>
where
    T: System
{
    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        use self::schema::blocks::dsl::{blocks, time};
        debug!("Inserting {} items via Batch Storage", self.inner().len());
        let storage: Vec<Storage<T>> = self.consume();
        let fut = db.run(move |conn| {
            for item in storage.into_iter() {
                let date_time = item.get_timestamp()?;
                diesel::update(blocks.find(item.hash().as_ref()))
                    .set(time.eq(Some(&date_time)))
                    .execute(&conn)
                    .map_err(|e| ArchiveError::from(e))?;
            }
            Ok(())
        }).map(|_| ());

        Ok(Box::new(fut))
    }
}

impl<T> Insert for Block<T>
where
    T: System + 'static
{

    fn insert(self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {

        let block = self.inner().block.clone();
        info!("HASH: {:X?}", block.header.hash().as_ref());
        info!("Block Num: {:?}", block.header.number());
        let extrinsics = get_extrinsics::<T>(block.extrinsics(), &block.header)?;
        // TODO Optimize
        let fut = db.run(move |conn| {
            trace!("Inserting Block: {:?}", block.clone());
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
            trace!("Inserting Extrinsics: {:?}", extrinsics);
            diesel::insert_into(inherents::table)
                .values(&extrinsics)
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

        let mut extrinsics: Vec<InsertInherentOwned> = Vec::new();
        info!("Batch inserting {} blocks into DB", self.inner().len());
        let blocks = self
            .inner()
            .iter()
            .map(|block| {
                let block = block.block.clone();
                let mut block_ext: Vec<InsertInherentOwned>
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
        let fut = db.run(move |conn| {
            for chunks in blocks.as_slice().chunks(10_000) {
                info!("{}", chunks.len());
                diesel::insert_into(blocks::table)
                    .values(chunks)
                    .execute(&conn)
                    .map_err(|e| ArchiveError::from(e))?;
            }
            for chunks in extrinsics.as_slice().chunks(10_000) {
                info!("{}", chunks.len());
                diesel::insert_into(inherents::table)
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
) -> Result<Vec<InsertInherentOwned>, ArchiveError>
where
    T: System
{
    debug!("Extrinsics: {:?}", extrinsics);
    extrinsics
        .iter()
        // enumerate is used here to preserve order/index of extrinsics
        .enumerate()
        .map(|(idx, x)| Ok(( idx, UncheckedExtrinsic::decode(&mut x.encode().as_slice())? )))
        .collect::<Vec<Result<(usize, BasicExtrinsic<T>), ArchiveError>>>()
        .into_iter()
        // we don't want to skip over _all_ extrinsics if decoding one extrinsic does not work
        .filter_map(|x: Result<(usize, BasicExtrinsic<T>), _>| {
            match x {
                Ok(v) => {
                    if let Some(b) = v.1.is_signed() {
                        if b {
                            // will be inserted as signed_extrinsic
                            None
                        } else {
                            debug!("Inherent: {:?}", v);
                            Some((v.0, v.1))
                        }
                    } else {
                        Some((v.0, v.1))
                    }
                },
                Err(e) => {
                    error!("{:?}", e);
                    None
                }
            }
        })
        .map(|(idx, decoded)| {
            let (module, call) = decoded.function.extract_call();
            let index: i32 = i32::try_from(idx)?;
            // info!("SubType: {:?}", call.is_sub_type());
            let res = call.function();
            let (fn_name, params);
            if res.is_err() {
                debug!("Call not Found, Call : {:?}", decoded);
                fn_name = format!("{:?}", decoded);
                params = Vec::new();
            } else {
                let (name, p) = res?;
                fn_name = name;
                params = p;
            }
            Ok(InsertInherentOwned {
                hash: header.hash().as_ref().to_vec(),
                block_num: (*header.number()).into(),
                module: module.to_string(),
                call: fn_name,
                parameters: Some(params),
                // success: true, // TODO: Success is not always true
                in_index: index
            })
        })
        .collect::<Result<Vec<InsertInherentOwned>, ArchiveError>>()
/*
    let fut = db.run(move |conn| {
        trace!("Inserting Extrinsics: {:?}", values);
        diesel::insert_into(inherents::table)
            .values(&values)
            .execute(&conn)
            .map_err(|e| e.into())
    }).map(|_| ());
    Ok(Box::new(fut))
*/
}


#[cfg(test)]
mod tests {
    //! Must be connected to a local database
    use super::*;




}
