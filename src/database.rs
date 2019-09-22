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
use tokio::runtime::Runtime;
use substrate_subxt::srml::system::System;
use diesel::{
    prelude::*,
    pg::PgConnection,
};
use runtime_primitives::traits::{Block, Header};
use dotenv::dotenv;
use std::env;

use crate::error::Error as ArchiveError;
use crate::types::{Data, Payload};
use self::models::{Blocks, /* Accounts, */ Inherants, SignedExtrinsics};

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

    pub fn insert<T>(&self, data: &Data<T>) where T: System {
        use self::schema::{accounts, blocks, inherants};

        match &data.payload {
            Payload::FinalizedHead(header) => {
                println!("Got a header")
            }
            Payload::Block(block) => {
                let block = Blocks {
                    parent_hash: block.block.header.parent_hash().into(),
                    hash: block.block.hash().into(),
                    block: block.block.header.number().into(),
                    state_root: block.block.header.state_root().into(),
                    extrinsics_root: block.block.header.extrinsics_root().into(),
                    time: None
                };
                diesel::insert_into(blocks::table)
                    .values(block)
                    .get_result(&self.connection)
                    .expect("ERROR saving block");
            },
            Payload::Event(event) => {
                println!("Event");
            },
            Payload::Hash(hash) => {
                println!("HASH: {:?}", hash);
            }
            _ => {
                println!("not handled");
            }
        }
    }
}
