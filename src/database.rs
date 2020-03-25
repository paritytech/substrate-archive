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
use diesel::{pg::PgConnection, prelude::*, sql_types::BigInt};
use dotenv::dotenv;
use log::*;
use runtime_primitives::traits::Header;

use codec::{Encode, Decode};
use desub::{decoder::{Decoder, GenericExtrinsic}, TypeDetective};

use std::{convert::TryFrom, env};

use crate::{
    database::{
        db_middleware::AsyncDiesel,
        models::{InsertBlock, InsertBlockOwned, InsertInherent, InsertTransaction, InsertInherentOwned, InsertTransactionOwned},
        schema::{blocks, inherents, signed_extrinsics},
    },
    error::Error as ArchiveError,
    queries,
    types::{BatchBlock, BatchStorage, Block, Data, Storage, Substrate},
};

pub type DbReturn = Result<(), ArchiveError>;

#[async_trait]
pub trait Insert<P: TypeDetective>: Sync {
    async fn insert(self, db: AsyncDiesel<PgConnection>, decoder: &Decoder<P>, spec: u32) -> DbReturn
    where
        Self: Sized;
}

#[async_trait]
impl<T, P> Insert<P> for Data<T>
where
    T: Substrate,
    P: TypeDetective,
{
    async fn insert(self, db: AsyncDiesel<PgConnection>, decoder: &Decoder<P>, spec: u32) -> DbReturn {
        match self {
            Data::Block(block) => block.insert(db, decoder, spec).await,
            Data::Storage(storage) => storage.insert(db, decoder, spec).await,
            Data::BatchBlock(blocks) => blocks.insert(db, decoder, spec).await,
            Data::BatchStorage(storage) => storage.insert(db, decoder, spec).await,
            o => Err(ArchiveError::UnhandledDataType),
        }
    }
}

/// Database object which communicates with Diesel in a (psuedo)asyncronous way
/// via `AsyncDiesel`
pub struct Database<P: TypeDetective> {
    db: AsyncDiesel<PgConnection>,
    decoder: Decoder<P>
}

impl<P: TypeDetective> Database<P> {
    /// Connect to the database
    pub fn new(decoder: Decoder<P>) -> Result<Self, ArchiveError> {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL")?;
        let db = AsyncDiesel::new(&database_url)?;
        Ok(Self { db, decoder })
    }

    pub async fn insert(&self, data: impl Insert<P>, spec: u32) -> Result<(), ArchiveError> 
    where
        P: TypeDetective,
    {
        data.insert(self.db.clone(), &self.decoder, spec).await
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
impl<T, P> Insert<P> for Storage<T>
where
    T: Substrate,
    P: TypeDetective
{
    async fn insert(self, db: AsyncDiesel<PgConnection>, decoder: &Decoder<P>, spec: u32) -> DbReturn {
        // use self::schema::blocks::dsl::{blocks, hash, time};
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
impl<T, P> Insert<P> for BatchStorage<T>
where
    T: Substrate,
    P: TypeDetective
{
    async fn insert(self, db: AsyncDiesel<PgConnection>, decoder: &Decoder<P>, spec: u32) -> DbReturn {
        use self::schema::blocks::dsl::{blocks, hash, time};
        debug!("Inserting {} items via Batch Storage", self.inner().len());
        let storage: Vec<Storage<T>> = self.consume();
        db.run(move |conn| {
            let len = storage.len();
            for item in storage.into_iter() {
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
impl<T, P> Insert<P> for Block<T>
where
    T: Substrate,
    P: TypeDetective
{
    async fn insert(self, db: AsyncDiesel<PgConnection>, decoder: &Decoder<P>, spec: u32) -> DbReturn {
        let block = self.inner().block.clone();
        info!("hash = {:X?}", block.header.hash().as_ref());
        info!("block_num = {:?}", block.header.number());
        let ext: Vec<GenericExtrinsic> = Vec::new(); 
        for val in block.extrinsics.iter() {
            ext.push(decoder.decode_extrinsic(spec, val.encode().as_slice())?)
        }
        let time = crate::util::try_to_get_time(ext.as_slice());
        // TODO Optimize, decode block number with desub 
        let num: u32 = Decode::decode(&mut block.header.number().encode().as_slice())?;
        db.run(move |conn| {
            diesel::insert_into(blocks::table)
                .values(InsertBlock {
                    parent_hash: block.header.parent_hash().as_ref(),
                    hash: block.header.hash().as_ref(),
                    block_num: &(num as i64),
                    state_root: block.header.state_root().as_ref(),
                    extrinsics_root: block.header.extrinsics_root().as_ref(),
                    time: time.as_ref(),
                })
                .on_conflict(blocks::hash)
                .do_nothing()
                .execute(&conn)?;

            let (mut signed_ext, mut unsigned_ext) = (Vec::new(), Vec::new());
            let len = ext.len() + 1; // 1 for the block
            
            for (i, e) in ext.into_iter().enumerate() {
                if e.is_signed() {
                    signed_ext.push(InsertTransaction {
                        block_num: &(num as i64),
                        hash: block.header.hash().as_ref(),
                        module: e.ext_module(),
                        call: e.ext_name(),
                        parameters: Some(serde_json::to_value(&e.args())?),
                        tx_index: &(i as i32),
                        transaction_version: &0
                    })
                } else {
                    unsigned_ext.push(InsertInherent {
                        hash: block.header.hash().as_ref(),
                        block_num: &(num as i64),
                        module: e.ext_module(),
                        call: e.ext_name(),
                        parameters: Some(&serde_json::to_value(&e.args())?),
                        in_index: &(i as i32),
                        // TODO: replace with real tx version
                        transaction_version: &0
                    })
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
impl<T, P> Insert<P> for BatchBlock<T>
where
    T: Substrate,
    P: TypeDetective
{
    async fn insert(self, db: AsyncDiesel<PgConnection>, decoder: &Decoder<P>, spec: u32) -> DbReturn {
        info!("Batch inserting {} blocks into DB", self.inner().len());
        let mut signed_ext = Vec::new();
        let mut unsigned_ext = Vec::new();
        let blocks = self
            .inner()
            .iter()
            .map(|block| {
                let block = block.block.clone();
                let ext: Vec<GenericExtrinsic> = Vec::new(); 
                for val in block.extrinsics.iter() {
                    ext.push(decoder.decode_extrinsic(spec, val.encode().as_slice())?)
                }
                log::debug!("Block Ext: {:?}", ext);
                let time = crate::util::try_to_get_time(ext.as_slice());
                let num: u32 = Decode::decode(&mut block.header.number().encode().as_slice())?;
                let db_block = InsertBlockOwned {
                    parent_hash: block.header.parent_hash().as_ref().to_vec(),
                    hash: block.header.hash().as_ref().to_vec(),
                    block_num: num as i64,
                    state_root: block.header.state_root().as_ref().to_vec(),
                    extrinsics_root: block.header.extrinsics_root().as_ref().to_vec(),
                    time,
                };
            
                for (i, e) in ext.into_iter().enumerate() {
                    if e.is_signed() {
                        signed_ext.push(InsertTransactionOwned {
                            block_num: num as i64,
                            hash: block.header.hash().as_ref().to_vec(),
                            module: e.ext_module().to_string(),
                            call: e.ext_name().to_string(),
                            //TODO UNWRAP!
                            parameters: Some(serde_json::to_value(&e.args()).unwrap()),
                            tx_index: i as i32,
                            transaction_version: 0
                        })
                    } else {
                        unsigned_ext.push(InsertInherentOwned {
                            hash: block.header.hash().as_ref().to_vec(),
                            block_num: num as i64,
                            module: e.ext_module().to_string(),
                            call: e.ext_name().to_string(),
                            //TODO UNWRAP!
                            parameters: Some(serde_json::to_value(&e.args()).unwrap()),
                            in_index: i as i32,
                            // TODO: replace with real tx version
                            transaction_version: 0
                        })
                    }
                }
                Ok((db_block))
            })
            // .filter_map(|b: Result<_, ArchiveError>| b.ok())
            .collect::<Result<Vec<InsertBlockOwned>, ArchiveError>>()?;


        // batch insert everything we've formatted/collected into the database 10,000 items at a time
        db.run(move |conn| {
            let len = blocks.len();
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
