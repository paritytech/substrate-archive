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

use runtime_primitives::{traits::Block, generic::UncheckedExtrinsic};
use diesel::{prelude::*, pg::PgConnection};
use codec::{Encode, Decode};
use runtime_primitives::traits::Header;
use runtime_support::dispatch::IsSubType;
use node_runtime::Call;
use dotenv::dotenv;
use std::env;

// use crate::error::Error as ArchiveError;
use crate::types::{Data, System, BasicExtrinsic};
use self::models::{InsertBlock, InsertInherent, Inherents, Blocks};
use self::schema::blocks;


/// Database object containing a postgreSQL connection and a runtime for asynchronous updating
pub struct Database {
    connection: PgConnection,
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

    pub fn insert<T>(&self, data: &Data<T>) where T: System
    {
        match &data {
            Data::FinalizedHead(_header) => {
            }
            Data::Block(block) => {
                println!("\n=====================================\n");
                let header = &block.block.header;
                let extrinsics = block.block.extrinsics();
                println!("HASH: {:X?}", header.hash().as_ref());
                for e in extrinsics.iter() {
                    let encoded = e.encode();
                    println!("Encoded Extrinsic: {:?}", encoded);
                    let decoded: Result<BasicExtrinsic<T>, _> = UncheckedExtrinsic::decode(&mut encoded.as_slice());
                    match decoded {
                        Err(e) => {
                            println!("{:?}", e);
                        },
                        Ok(v) => {
                            println!("{:?}", v.function);
                            let encoded = v.function.encode();
                            // let dec: String = Decode::decode(&mut &encoded[0..3]).unwrap();
                            println!("encoded function: {:?}", v.function.encode());
                            let decoded = Call::decode(&mut v.function.encode().as_slice());
                            println!("Decoded Function: {:?}", decoded);

                            // basic calls

                            // println!("{:?}", v.function.is_aux_sub_type());
                            /*
                            match &v {
                                <T as System>::Call::Timestamp(epoch) => {
                                    println!("the timestamp is {:?}", epoch);
                                }
                            };
                            */
                            /*
                            if v.is_signed() {
                                unimplemented!();
                            } else {
                                /*
                                diesel::insert_into(inherents::table)
                                    .values( InsertInherent {
                                        hash: header.hash().as_ref(),
                                        block: &(*header.number()).into(),
                                        module: "test",
                                        call: "test",
                                        success: true,

                                    })
                                 */

                            }
                            */
                        }
                    };
                    println!("opaque ext: {:?}", e);
                }
                println!("Inserting");
                diesel::insert_into(blocks::table)
                    .values( InsertBlock {
                        parent_hash: header.parent_hash().as_ref(),
                        hash: header.hash().as_ref(),
                        block: &(*header.number()).into(),
                        state_root: header.state_root().as_ref(),
                        extrinsics_root: header.extrinsics_root().as_ref(),
                        time: None
                    })
                    .get_result::<Blocks>(&self.connection)
                    .expect("ERROR saving block");
            },
            Data::Event(_event) => {
            },
            /*Data::Hash(hash) => {
                println!("HASH: {:?}", hash);
            }*/
            _ => {
            }
        }
        println!("\n ================================ \n");
    }
}

/*
// TODO: Optimize
#[derive(Debug, Clone, PartialEq)]
pub struct Extrinsic {
    address: Vec<u8>,
    index: Option<Vec<u8>>,
    call: Option<Vec<u8>>,
    signature: Option<Vec<u8>>
}
*/
