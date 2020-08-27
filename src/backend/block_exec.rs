// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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

use crate::{error::Result, types::Storage};
use sc_client_api::backend;
use sp_api::{ApiExt, ApiRef};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header, NumberFor},
};
use sp_storage::{StorageData, StorageKey as StorageKeyWrapper};
use std::sync::Arc;

pub type StorageKey = Vec<u8>;
pub type StorageValue = Vec<u8>;
pub type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;
pub type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

/// Storage Changes that occur as a result of a block's executions
#[derive(Clone, Debug)]
pub struct BlockChanges<Block: BlockT> {
    /// In memory array of storage values.
    pub storage_changes: StorageCollection,
    /// In memory arrays of storage values for multiple child tries.
    pub child_storage: ChildStorageCollection,
    /// Hash of the block these changes come from
    pub block_hash: Block::Hash,
    pub block_num: NumberFor<Block>,
}

impl<B: BlockT> xtra::Message for BlockChanges<B> {
    // TODO: possibly change this error
    type Result = ();
}

impl<Block> From<BlockChanges<Block>> for Storage<Block>
where
    Block: BlockT,
    NumberFor<Block>: Into<u32>,
{
    fn from(changes: BlockChanges<Block>) -> Storage<Block> {
        let hash = changes.block_hash;
        let num: u32 = changes.block_num.into();

        Storage::new(
            hash,
            num,
            false,
            changes
                .storage_changes
                .into_iter()
                .map(|s| (StorageKeyWrapper(s.0), s.1.map(StorageData)))
                .collect::<Vec<(StorageKeyWrapper, Option<StorageData>)>>(),
        )
    }
}

pub struct BlockExecutor<'a, Block, Api, B>
where
    Block: BlockT,
    Api: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>,
{
    api: ApiRef<'a, Api>,
    backend: &'a Arc<B>,
    block: Block,
    id: BlockId<Block>,
}

impl<'a, Block, Api, B> BlockExecutor<'a, Block, Api, B>
where
    Block: BlockT,
    // Api: ProvideRuntimeApi<Block>,
    Api: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>,
{
    pub fn new(api: ApiRef<'a, Api>, backend: &'a Arc<B>, block: Block) -> Result<Self> {
        let header = block.header();
        let parent_hash = header.parent_hash();
        let id = BlockId::Hash(*parent_hash);

        Ok(Self {
            api,
            backend,
            block,
            id,
        })
    }

    pub fn block_into_storage(self) -> Result<BlockChanges<Block>> {
        let header = (&self.block).header();
        let parent_hash = *header.parent_hash();
        let hash = header.hash();
        let num = *header.number();

        let state = self.backend.state_at(self.id)?;

        // FIXME: For some reason, wasm runtime calculates a different number of digest items
        // then what we have in the block
        // We don't do anything with consensus
        // so digest isn't very important (we don't currently index digest items anyway)
        // popping a digest item has no effect on storage changes afaik
        let (mut header, ext) = self.block.deconstruct();
        header.digest_mut().pop();
        let block = Block::new(header, ext);

        self.api.execute_block(&self.id, block)?;
        let storage_changes = self.api.into_storage_changes(&state, None, parent_hash)?;

        Ok(BlockChanges {
            storage_changes: storage_changes.main_storage_changes,
            child_storage: storage_changes.child_storage_changes,
            block_hash: hash,
            block_num: num,
        })
    }
}
