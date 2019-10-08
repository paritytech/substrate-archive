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

use log::*;
use failure::Error;
use runtime_primitives::{traits::Block, generic::UncheckedExtrinsic};
use diesel::{prelude::*, pg::PgConnection};
use codec::{Encode, Decode};
use runtime_primitives::traits::Header;
use dotenv::dotenv;
use std::env;
use std::convert::TryFrom;

use crate::error::Error as ArchiveError;
use crate::types::{Data, System, BasicExtrinsic, ExtractCall};
use self::models::{InsertBlock, InsertInherent, Inherents, Blocks};
use self::schema::{blocks, inherents};


/// Database object containing a postgreSQL connection and a runtime for asynchronous updating
pub struct Database {
    connection: PgConnection,
}

impl Database {

    /// Connect to the database
    pub fn new() -> Result<Self, ArchiveError> {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL")?;
        let connection = PgConnection::establish(&database_url)?;

        Ok(Self { connection })
    }

    // TODO: make async
    pub fn insert<T>(&self, data: &Data<T>) -> Result<(), Error>
    where T: System
    {
        match &data {
            Data::Block(block) => {
                let header = &block.block.header;
                let extrinsics = block.block.extrinsics();
                info!("HASH: {:X?}", header.hash().as_ref());
                diesel::insert_into(blocks::table)
                    .values( InsertBlock {
                        parent_hash: header.parent_hash().as_ref(),
                        hash: header.hash().as_ref(),
                        block: &(*header.number()).into(),
                        state_root: header.state_root().as_ref(),
                        extrinsics_root: header.extrinsics_root().as_ref(),
                        time: None
                    })
                    .get_result::<Blocks>(&self.connection)?;
                for (idx, e) in extrinsics.iter().enumerate() {
                    //TODO possibly redundant operation
                    let encoded = e.encode();
                    let decoded: BasicExtrinsic<T> = UncheckedExtrinsic::decode(&mut encoded.as_slice())?;
                    let (module, call) = decoded.function.extract_call();
                    let (fn_name, params) = call.function()?;
                    diesel::insert_into(inherents::table)
                        .values( InsertInherent {
                            hash: header.hash().as_ref(),
                            block: &(*header.number()).into(),
                            module: &String::from(module),
                            call: &fn_name,
                            parameters: Some(params),
                            success: &true,
                            in_index: &(i32::try_from(idx)?)
                        } )
                        .get_result::<Inherents>(&self.connection)?;
                }
            },
            Data::Storage(data, from, hash) => {

                println!("{:?}, {:?}, {:?}", data, from, hash);
            }
            _ => {
            }
        }
        Ok(())
    }
}
