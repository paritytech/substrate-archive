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

use crate::database::Database;
use crate::types::*;
use bastion::prelude::*;
use subxt::system::System;

pub const REDUNDANCY: usize = 5;

pub fn actor<T>(db: Database) -> Result<ChildrenRef, ()>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    Bastion::children(|children: Children| {
        children
            .with_redundancy(REDUNDANCY)
            .with_exec(move |ctx: BastionContext| {
                let db = db.clone();
                async move {
                    loop {
                        msg! {
                            ctx.recv().await?,
                            block: Block<T> =!> {
                                process_block(&db, block).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't Answer");
                            };
                            blocks: Vec<Block<T>> =!> {
                                log::info!("Inserting {} blocks into the database!EEEEEEEEEEEEEEEEEEEEEEEEEE", blocks.len());
                                process_blocks(&db, blocks).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            extrinsics: Vec<SignedExtrinsic<T>> =!> {
                                process_extrinsics(&db, extrinsics).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            inherents: Vec<Inherent<T>> =!> {
                                process_inherents(&db, inherents).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        };
                    }
                }
            })
    })
}

async fn process_block<T>(db: &Database, block: Block<T>)
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    match db.insert(block).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}

async fn process_blocks<T>(db: &Database, blocks: Vec<Block<T>>)
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    match db.insert(BatchBlock::new(blocks)).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}

async fn process_extrinsics<T>(db: &Database, extrinsics: Vec<SignedExtrinsic<T>>)
where
    T: Substrate + Send + Sync,
{
    match db.insert(extrinsics).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}

async fn process_inherents<T>(db: &Database, inherents: Vec<Inherent<T>>)
where
    T: Substrate + Send + Sync,
{
    match db.insert(inherents).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e)
    }
}
