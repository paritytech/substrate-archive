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
use crate::error::Error as ArchiveError;
use bastion::prelude::*;
use crate::print_on_err;

pub const REDUNDANCY: usize = 16;


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
                                print_on_err!(process_block(&db, block).await);
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't Answer");
                            };
                            blocks: Vec<Block<T>> =!> {
                                log::info!("Inserting {} blocks", blocks.len());
                                print_on_err!(process_blocks(&db, blocks).await);
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            extrinsics: Vec<Extrinsic<T>> =!> {
                                print_on_err!(process_extrinsics(&db, extrinsics).await);
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            metadata: Metadata =!> {
                                print_on_err!(process_metadata(&db, metadata).await);
                                answer!(ctx, super::ArchiveAnswer::Success).expect("Couldn't answer");
                            };
                            storage: Vec<Storage<T>> => {
                                log::info!("Inserting {} storage entries", storage.len());
                                print_on_err!(process_storage(&db, storage).await);
                            };
                            ref broadcast: super::Broadcast => {
                                match broadcast {
                                    super::Broadcast::Shutdown => {
                                        break;
                                    }
                                }
                            };
                            e: _ => log::warn!("Received unknown data {:?}", e);
                        };
                    }
                    Ok(())
                }
            })
    })
}

async fn process_block<T>(db: &Database, block: Block<T>)
    -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    while !queries::check_if_meta_exists(block.spec, db.pool())
        .await?
    {
        timer::Delay::new(std::time::Duration::from_millis(20)).await;
    }
    db.insert(block).await.map(|_| ())
}

async fn process_blocks<T>(db: &Database, blocks: Vec<Block<T>>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    log::info!("Got {} blocks", blocks.len());
    let mut specs = blocks.clone();
    specs.as_mut_slice().sort_by_key(|b| b.spec);
    let mut specs = specs.into_iter().map(|b| b.spec).collect::<Vec<u32>>();
    specs.dedup();
    loop {
        let versions = queries::get_versions(db.pool()).await?;
        if db_contains_metadata(specs.as_slice(), versions) {
            break;
        }
        timer::Delay::new(std::time::Duration::from_millis(20)).await;
    }

    db.insert(BatchBlock::new(blocks)).await.map(|_| ())
}

fn db_contains_metadata(specs: &[u32], versions: Vec<crate::queries::Version>) -> bool {
    let versions = versions
        .into_iter()
        .map(|v| v.version as u32)
        .collect::<Vec<u32>>();
    for spec in specs.iter() {
        if !versions.contains(spec) {
            return false
        }
    }
    return true
}

async fn process_extrinsics<T>(db: &Database, extrinsics: Vec<Extrinsic<T>>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    db.insert(extrinsics).await.map(|_| ())
}

async fn process_metadata(db: &Database, meta: Metadata) -> Result<(), ArchiveError> {
    db.insert(meta).await.map(|_| ())
}

async fn process_storage<T>(db: &Database, storage: Vec<Storage<T>>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
{
    db.insert(storage).await.map(|_| ())
}
