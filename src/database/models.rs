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

//! Direct Database Type representations of types in `types.rs`
//! Only some types implemented, for convenience most types are already in their database model
//! equivalents

use crate::actors::msg;
use crate::types::*;
use serde::{Deserialize, Serialize};
use sp_runtime::traits::Block as BlockT;
use sp_storage::{StorageData, StorageKey};

/// Struct modeling data returned from database when querying for a block
#[derive(sqlx::FromRow, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct BlockModel {
    pub id: i32,
    pub parent_hash: Vec<u8>,
    pub hash: Vec<u8>,
    pub block_num: i32,
    pub state_root: Vec<u8>,
    pub extrinsics_root: Vec<u8>,
    pub digest: Vec<u8>,
    pub ext: Vec<u8>,
    pub spec: i32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StorageModel<Block: BlockT> {
    hash: Block::Hash,
    block_num: u32,
    full_storage: bool,
    key: StorageKey,
    data: Option<StorageData>,
}

impl<Block: BlockT> StorageModel<Block> {
    pub fn new(
        hash: Block::Hash,
        block_num: u32,
        full_storage: bool,
        key: StorageKey,
        data: Option<StorageData>,
    ) -> Self {
        Self {
            block_num,
            hash,
            full_storage,
            key,
            data,
        }
    }

    pub fn is_full(&self) -> bool {
        self.full_storage
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }

    pub fn hash(&self) -> &Block::Hash {
        &self.hash
    }

    pub fn key(&self) -> &StorageKey {
        &self.key
    }

    pub fn data(&self) -> Option<&StorageData> {
        self.data.as_ref()
    }
}

impl<Block: BlockT> From<Storage<Block>> for Vec<StorageModel<Block>> {
    fn from(original: Storage<Block>) -> Vec<StorageModel<Block>> {
        let hash = *original.hash();
        let block_num = original.block_num();
        let full_storage = original.is_full();
        original
            .changes
            .into_iter()
            .map(|changes| StorageModel::new(hash, block_num, full_storage, changes.0, changes.1))
            .collect::<Vec<StorageModel<Block>>>()
    }
}

impl<Block: BlockT> From<msg::VecStorageWrap<Block>> for Vec<StorageModel<Block>> {
    fn from(original: msg::VecStorageWrap<Block>) -> Vec<StorageModel<Block>> {
        original
            .0
            .into_iter()
            .flat_map(Vec::<StorageModel<Block>>::from)
            .collect()
    }
}
