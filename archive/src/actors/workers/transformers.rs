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
use crate::{error::Error as ArchiveError, types::*};
use sp_core::storage::StorageChangeSet;
use xtra::prelude::*;

// FIXME: This actor is sort of useless. We're not decoding anything.

#[derive(Default)]
pub struct Transform;

impl Actor for Transform {}

pub struct StorageWrap<T: Substrate>(<T as System>::BlockNumber, StorageChangeSet<<T as System>::Hash>);

impl<T: Substrate> From<(<T as System>::BlockNumber, StorageChangeSet<<T as System>::Hash>)>
    for StorageWrap<T>
{
    fn from(v: (<T as System>::BlockNumber, StorageChangeSet<<T as System>::Hash>)) -> StorageWrap<T> {
        StorageWrap(v.0, v.1)
    }
}

pub struct VecStorageWrap<T: Substrate>(Vec<Storage<T>>);

impl<T: Substrate> From<Vec<Storage<T>>> for VecStorageWrap<T> {
    fn from(v: Vec<Storage<T>>) -> VecStorageWrap<T> {
        VecStorageWrap(v)
    }
}

impl<T: Substrate> Message for StorageWrap<T> {
    type Result = Result<(), ArchiveError>;
}

impl<T: Substrate> Message for VecStorageWrap<T> {
    type Result = Result<(), ArchiveError>;
}

#[async_trait::async_trait]
impl<T> Handler<Block<T>> for Transform
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn handle(&mut self, blk: Block<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        process_block::<T>(blk).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Handler<BatchBlock<T>> for Transform
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn handle(&mut self, blks: BatchBlock<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        process_blocks::<T>(blks).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Handler<StorageWrap<T>> for Transform
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn handle(&mut self, stg: StorageWrap<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        let (num, changes) = (stg.0, stg.1);
        process_storage::<T>(num, changes).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Handler<VecStorageWrap<T>> for Transform
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    async fn handle(&mut self, stg: VecStorageWrap<T>, _ctx: &mut Context<Self>) -> Result<(), ArchiveError> {
        for s in stg.0.into_iter() {
            // send to database
            // debug
        }
        Ok(())
    }
}

pub async fn process_storage<T>(
    num: <T as System>::BlockNumber,
    changes: StorageChangeSet<<T as System>::Hash>,
) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    let hash = changes.block;
    let num: u32 = num.into();
    let storage = Storage::<T>::new(hash, num, false, changes.changes);
    // let v = sched.ask_next("db", storage)?.await;
    // log::debug!("{:?}", v);
    Ok(())
}

pub async fn process_block<T>(block: Block<T>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    // blocks need to be inserted before extrinsics, so that extrinsics may reference block hash in postgres
    // let v = sched.ask_next("db", block)?.await;
    // log::debug!("{:?}", v);
    println!("Should insert now!");
    Ok(())
}

pub async fn process_blocks<T>(blocks: BatchBlock<T>) -> Result<(), ArchiveError>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u32>,
{
    log::info!("Got {} blocks", blocks.inner().len());

    // let batch_blocks = BatchBlock::new(blocks);
    // blocks need to be inserted before extrinsics, so that extrinsics may reference block hash in postgres
    log::info!("Processing blocks");
    // let v = sched.ask_next("db", batch_blocks)?.await;
    // log::debug!("{:?}", v);
    Ok(())
}
