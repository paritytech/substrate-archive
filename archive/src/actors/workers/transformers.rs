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

//! Actors which do work by transforming data before it's inserted into the database
//! These actors do not make any external connections to a Database or Network
use crate::{error::ArchiveResult, types::*};
use sp_core::storage::StorageChangeSet;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use xtra::prelude::*;

// FIXME: This actor is sort of useless. We're not decoding anything.

#[derive(Default)]
pub struct Transform;

impl Actor for Transform {}

pub struct StorageWrap<B: BlockT>(NumberFor<B>, StorageChangeSet<B::Hash>);

impl<B: BlockT> From<(NumberFor<B>, StorageChangeSet<B::Hash>)> for StorageWrap<B> {
    fn from(v: (NumberFor<B>, StorageChangeSet<B::Hash>)) -> StorageWrap<B> {
        StorageWrap(v.0, v.1)
    }
}

pub struct VecStorageWrap<B: BlockT>(Vec<Storage<B>>);

impl<B: BlockT> From<Vec<Storage<B>>> for VecStorageWrap<B> {
    fn from(v: Vec<Storage<B>>) -> VecStorageWrap<B> {
        VecStorageWrap(v)
    }
}

impl<B: BlockT> Message for StorageWrap<B> {
    type Result = ArchiveResult<()>;
}

impl<B: BlockT> Message for VecStorageWrap<B> {
    type Result = ArchiveResult<()>;
}

#[async_trait::async_trait]
impl<B> Handler<Block<B>> for Transform
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blk: Block<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        // blocks need to be inserted before extrinsics, so that extrinsics may reference block hash in postgres
        // let v = sched.ask_next("db", block)?.await;
        // log::debug!("{:?}", v);
        println!("Should insert now!");
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B> Handler<BatchBlock<B>> for Transform
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, blks: BatchBlock<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        log::info!("Got {} blocks", blks.inner().len());

        // let batch_blocks = BatchBlock::new(blocks);
        // blocks need to be inserted before extrinsics, so that extrinsics may reference block hash in postgres
        log::info!("Processing blocks");
        // let v = sched.ask_next("db", batch_blocks)?.await;
        // log::debug!("{:?}", v);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B> Handler<StorageWrap<B>> for Transform
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, stg: StorageWrap<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        let (num, changes) = (stg.0, stg.1);
        process_storage::<B>(num, changes).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<B> Handler<VecStorageWrap<B>> for Transform
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    async fn handle(&mut self, stg: VecStorageWrap<B>, _ctx: &mut Context<Self>) -> ArchiveResult<()> {
        for s in stg.0.into_iter() {
            // send to database
            // debug
        }
        Ok(())
    }
}

pub async fn process_storage<B>(num: NumberFor<B>, changes: StorageChangeSet<B::Hash>) -> ArchiveResult<()>
where
    B: BlockT,
    NumberFor<B>: Into<u32>,
{
    let hash = changes.block;
    let num: u32 = num.into();
    let storage = Storage::<B>::new(hash, num, false, changes.changes);
    // let v = sched.ask_next("db", storage)?.await;
    // log::debug!("{:?}", v);
    Ok(())
}
