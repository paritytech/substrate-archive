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
use runtime_primitives::{traits::Block as BlockTrait, OpaqueExtrinsic, generic::UncheckedExtrinsic};
use diesel::{prelude::*, pg::PgConnection};
use codec::{Encode, Decode};
use runtime_primitives::traits::Header;
use dotenv::dotenv;
use chrono::offset::{Utc, TimeZone};
use std::{
    env,
    convert::TryFrom
};
use crate::{
    util,
    error::Error as ArchiveError,
    types::{Data, System, Block, Storage, BasicExtrinsic, ExtractCall},
    database::{
        models::{InsertBlock, InsertInherent, InsertInherentOwned, Inherents, Blocks},
        schema::{blocks, inherents}
    },
};
use self::db_middleware::AsyncDiesel;

pub type DbFuture = Box<dyn Future<Item = (), Error = ArchiveError> + Send >;

pub trait Insert {
    type Error;
    fn insert(&self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, Self::Error>;
}

impl<T> Insert for Data<T> where T: System + 'static {
    type Error = ArchiveError;
    fn insert(&self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        match self {
            Data::Block(block) => {
                block.insert(db)
            },
            Data::Storage(storage) => {
                storage.insert(db)
            },
            _=> {
                Err(ArchiveError::UnhandledDataType)
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

    pub fn insert<T>(&self, data: &Data<T>) -> DbFuture
    where
        T: System + 'static
    {
        let result = data.insert(self.db.clone());
        match result {
            Ok(v) => v,
            Err(e) => Box::new(future::err(e)),
        }
    }
}

impl<T> Insert for Storage<T>
where
    T: System,
{
    type Error = ArchiveError;

    fn insert(&self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        use self::schema::blocks::dsl::{blocks, time};
        let (data, key_type, hash) = (self.data(), self.key_type(), self.hash().clone());
        let unix_time: i64 = Decode::decode(&mut data.0.as_slice())?;
        let date_time = Utc.timestamp_millis(unix_time); // panics if time is incorrect
        let fut = db.run(move |conn| {
            diesel::update(blocks.find(hash.as_ref()))
                .set(time.eq(Some(&date_time)))
                .execute(&conn)
                .map_err(|e| e.into())
        }).map(|_| ());
        Ok(Box::new(fut))
    }
}

impl<T> Insert for Block<T>
where
    T: System + 'static
{
    type Error = ArchiveError;

    fn insert(&self, db: AsyncDiesel<PgConnection>) -> Result<DbFuture, ArchiveError> {
        let block_1 = self.inner().block.clone(); // TODO maybe try to do this with references not moving
        let block_2 = self.inner().block.clone();
        info!("HASH: {:X?}", block_1.header.hash().as_ref());
        let fut = db.run(move |conn| {
            let block = block_1;
            // let header = header.clone();
            // let extrinsics = extrinsics.clone();
            diesel::insert_into(blocks::table)
                .values( InsertBlock {
                    parent_hash: block.header.parent_hash().as_ref(),
                    hash: block.header.hash().as_ref(),
                    block: &(*block.header.number()).into(),
                    state_root: block.header.state_root().as_ref(),
                    extrinsics_root: block.header.extrinsics_root().as_ref(),
                    time: None
                })
                .execute(&conn)
                .map_err(|e| e.into())
        }).and_then(move |res| {
            let block = block_2;
            insert_extrinsics::<T>(block.extrinsics().to_vec(), block.header, db)
        }).map(|_| ());
        Ok(Box::new(fut))
    }
}

fn insert_extrinsics<T>(
    extrinsics: Vec<OpaqueExtrinsic>,
    header: T::Header,
    db: AsyncDiesel<PgConnection>,
) -> Result<DbFuture, ArchiveError>
where
    T: System + 'static
{
    let mut values = Vec::new();

    let values = extrinsics
        .iter()
        // enumerate is used here to preserve order/index of extrinsics
        .enumerate()
        .map(|(idx, x)| UncheckedExtrinsic::decode(&mut x.encode().as_slice()))
        .collect::<Vec<Result<BasicExtrinsic<T>, _>>>()
        // we don't want to skip over *all* extrinsics if decoding one extrinsic does not work
        .filter_map(|(idx, x)| {
            match x {
                Ok(v) => {
                    Some((idx, v))
                },
                Err(e) => {
                    error!("{:?}", e);
                    None
                }
            }
        })
        .for_each(|(idx, decoded)| {
            let (module, call) = decoded.function.extract_call();
            call.function();
            call.function.and_then(|(fn_name, params)| {
                InsertInherentOwned {
                    hash: header.hash().as_ref().to_vec(),
                    block: (*header.number()).into(),
                    module: module.into(),
                    call: fn_name,
                    parameters: Some(params),
                    success: true,
                    in_index: i32::try_from(idx)?
                }
            })
        })
        .collect::<Vec<InsertInherentOwned>>();

    let fut = db.run(move |conn| {
        diesel::insert_into(inherents::table)
            .values(&values)
            .execute(&conn)
            .map_err(|e| e.into())
    }).map(|_| ());
    Ok(Box::new(fut))
}
