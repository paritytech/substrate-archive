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
use crate::queries;
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
                                log::info!("Inserting {} blocks", blocks.len());
                                process_blocks(&db, blocks).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            extrinsics: Vec<Extrinsic<T>> =!> {
                                log::info!("Inserting {} extrinsics", extrinsics.len());
                                process_extrinsics(&db, extrinsics).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            metadata: Metadata =!> {
                                process_metadata(&db, metadata).await;
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            storage: Vec<Storage<T>> =!> {
                                log::info!("Inserting {} storage entries", storage.len());
                                process_storage(&db, storage).await;
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
    while !queries::check_if_meta_exists(block.spec, db.pool())
        .await
        .unwrap()
    {
        async_std::task::sleep(std::time::Duration::from_millis(10)).await;
    }
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
    log::info!("Got {} blocks", blocks.len());
    let mut specs = blocks.clone();
    specs.as_mut_slice().sort_by_key(|b| b.spec);
    let mut specs = specs.into_iter().map(|b| b.spec).collect::<Vec<u32>>();
    specs.dedup();
    'meta: loop {
        let versions = match queries::get_versions(db.pool()).await {
            Ok(v) => v,
            Err(e) => {
                log::error!("{:?}", e);
                panic!("Error");
            }
        };
        if db_contains_metadata(specs.as_slice(), versions) {
            break 'meta;
        }
        async_std::task::sleep(std::time::Duration::from_millis(20)).await;
    }

    match db.insert(BatchBlock::new(blocks)).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}

fn db_contains_metadata(specs: &[u32], versions: Vec<crate::queries::Version>) -> bool {
    let versions = versions
        .into_iter()
        .map(|v| v.version as u32)
        .collect::<Vec<u32>>();
    for spec in specs.iter() {
        if !versions.contains(spec) {
            return false;
        }
    }
    return true;
}

async fn process_extrinsics<T>(db: &Database, extrinsics: Vec<Extrinsic<T>>)
where
    T: Substrate + Send + Sync,
{
    match db.insert(extrinsics).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}

async fn process_metadata(db: &Database, meta: Metadata) {
    match db.insert(meta).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}

async fn process_storage<T>(db: &Database, storage: Vec<Storage<T>>)
where
    T: Substrate + Send + Sync,
{
    match db.insert(storage).await {
        Ok(_) => (),
        Err(e) => log::error!("{:?}", e),
    }
}
