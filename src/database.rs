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
// use substrate_subxt::srml::system::System;
use diesel::{
    prelude::*,
    pg::PgConnection,
    serialize::ToSql,
    expression::AsExpression,
    sql_types::BigInt
};
use codec::{Encode, Compact};
use runtime_primitives::traits::Header;
use dotenv::dotenv;
use std::env;
use std::convert::TryFrom;

// use crate::error::Error as ArchiveError;
use crate::types::{Data, System};
use self::models::{InsertBlock, Blocks};
use self::schema::blocks;

/// Database object containing a postgreSQL connection and a runtime for asynchronous updating
pub struct Database {
    connection: PgConnection
}

impl Database {

    /// Connect to the database
    pub fn new() -> Self {
        dotenv().ok();

        let database_url = env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set; qed");
        let connection = PgConnection::establish(&database_url)
            .expect(&format!("Error connecting to {}", database_url));

        Self { connection }
    }

    pub fn insert<T>(&self, data: &Data<T>)
    where
        T: System,
    {
        match &data {
            Data::FinalizedHead(_header) => {
                println!("Got a header")
            }
            Data::Block(block) => {
                let block = &block.block.header;
                diesel::insert_into(blocks::table)
                    .values( InsertBlock {
                        parent_hash: block.parent_hash().as_ref(),
                        hash: block.hash().as_ref(),
                        block: &(*block.number()).into(),
                        state_root: block.state_root().as_ref(),
                        extrinsics_root: block.extrinsics_root().as_ref(),
                        time: None
                    })
                    .get_result::<Blocks>(&self.connection)
                    .expect("ERROR saving block");
                println!("Block");

            },
            Data::Event(_event) => {
                println!("Event");
            },
            /*Data::Hash(hash) => {
                println!("HASH: {:?}", hash);
            }*/
            _ => {
                println!("not handled");
            }
        }
    }
}
