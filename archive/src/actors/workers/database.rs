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

use crate::database::{models::*, Database};
use crate::error::ArchiveResult;
use crate::queries;
use crate::types::*;
use xtra::prelude::*;

pub struct DatabaseActor {
    db: Database,
}

impl Actor for DatabaseActor {}

#[async_trait::async_trait]
impl<T> Handler<Block<T>> for DatabaseActor
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn handle(&mut self, blk: Block<T>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        while !queries::check_if_meta_exists(blk.spec, self.db.pool()).await? {
            timer::Delay::new(std::time::Duration::from_millis(20)).await;
        }
        self.db.insert(blk).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl<T> Handler<BatchBlock<T>> for DatabaseActor
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn handle(&mut self, blks: BatchBlock<T>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let mut specs = blks.inner().clone();
        specs.as_mut_slice().sort_by_key(|b| b.spec);
        let mut specs = specs.into_iter().map(|b| b.spec).collect::<Vec<u32>>();
        specs.dedup();
        loop {
            let versions = queries::get_versions(self.db.pool()).await?;
            if db_contains_metadata(specs.as_slice(), versions) {
                break;
            }
            timer::Delay::new(std::time::Duration::from_millis(50)).await;
        }

        self.db.insert(blks).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl Handler<Metadata> for DatabaseActor {
    async fn handle(&mut self, meta: Metadata, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        self.db.insert(meta).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl<T: Substrate> Handler<Storage<T>> for DatabaseActor {
    async fn handle(&mut self, storage: Storage<T>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        while !queries::check_if_block_exists(storage.hash().as_ref(), self.db.pool()).await? {
            timer::Delay::new(std::time::Duration::from_millis(10)).await;
        }
        self.db
            .insert(Vec::<StorageModel<T>>::from(storage))
            .await
            .map(|_| ())
    }
}

// Returns true if all versions are in database
// false if versions are missing
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
