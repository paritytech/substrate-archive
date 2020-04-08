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
use crate::{
    database::Database,
    error::Error as ArchiveError,
    rpc::Rpc,
    types::{BatchBlock, BatchData, ChainInfo, Substrate},
};
use desub::TypeDetective;
use futures::channel::mpsc::UnboundedSender;
use runtime_primitives::traits::{Block as _, Header as _};
use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};
use substrate_rpc_primitives::number::NumberOrHex;
use subxt::system::System;

pub struct BlocksArchive<T: Substrate + Send + Sync, P: TypeDetective> {
    rpc: Arc<Rpc<T>>,
    db: Arc<Database<P>>,
    queue: Arc<RwLock<HashSet<<T as System>::Hash>>>,
}

impl<T, P> BlocksArchive<T, P>
where
    T: Substrate + Send + Sync,
    <T as System>::BlockNumber: Into<u64>,
    <T as System>::BlockNumber: From<u64>,
    P: TypeDetective + Send + Sync,
    <T as System>::Hash: std::hash::Hash,
{
    pub fn new(
        queue: Arc<RwLock<HashSet<<T as System>::Hash>>>,
        rpc: Arc<Rpc<T>>,
        db: Arc<Database<P>>,
    ) -> Result<Self, ArchiveError> {
        Ok(Self { rpc, db, queue })
    }

    /// archives missing blocks and associated extrinsics/inherents
    /// inserts missing block hashes into 'queue'
    pub async fn run(self, handler: UnboundedSender<BatchData<T>>) -> Result<(), ArchiveError> {
        let latest = self
            .rpc
            .latest_block()
            .await?
            .ok_or(ArchiveError::from("Block"))?;
        let latest = latest.block.header().number();
        let latest_block: u64 = (*latest).into();
        let missing = self.db.query_missing_blocks(Some(latest_block))?;
        let blocks = self
            .rpc
            .batch_block_from_number(
                missing
                    .into_iter()
                    .map(|b| NumberOrHex::Number(b.into()))
                    .collect::<Vec<NumberOrHex<<T as System>::BlockNumber>>>(),
            )
            .await?;
        {
            let mut queue = self.queue.write()?;
            for block in blocks.iter() {
                queue.insert(block.get_hash());
            }
        }
        handler.unbounded_send(BatchData::BatchBlock(BatchBlock::new(blocks)))?;
        Ok(())
    }
}
