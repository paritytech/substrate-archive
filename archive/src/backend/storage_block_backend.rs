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


//! Executes blocks to get storage changes without traversing changes trie

use crate::{
    backend::ChainAccess,
    error::Error as ArchiveError,
    types::*
};
use std::sync::Arc;
use sc_client_db::Backend;
use sc_client_api::backend::{self, Backend as _};
use sp_api::{ApiExt, ApiErrorFor, ApiRef, ProvideRuntimeApi, StorageChanges};
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, HashFor}
};

/// Storage Changes that occur as a result of a block's executions
pub struct BlockChanges<Block: BlockT, StateBackend: backend::StateBackend<HashFor<Block>>> {
	/// The changes that need to be applied to the backend to get the state of the build block.
	pub storage_changes: StorageChanges<StateBackend, Block>,
}

pub struct BlockExecutor<'a, Block: BlockT, Api: ProvideRuntimeApi<Block>, B> {
    api: ApiRef<'a, Api::Api>,
    backend: &'a B
}

impl<'a, Block, Api, B> BlockExecutor<'a, Block, Api, B>
where
    Block: BlockT,
    Api: ProvideRuntimeApi<Block>,
    Api::Api: ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>,
{
    pub fn new(api: &'a Api, backend: &'a B) -> Self {
        let api = api.runtime_api();

        Self {
            api, backend
        }
    }

    pub fn block_into_storage(&self, block_id: BlockId<Block>, parent_hash: Block::Hash
    ) -> Result<BlockChanges<Block, backend::StateBackendFor<B, Block>>, ArchiveError> {

        let state = self.backend.state_at(block_id)?;

        let changes_trie_state = backend::changes_tries_state_at_block(
			&block_id,
			self.backend.changes_trie_storage(),
		)?;

		let storage_changes = self.api.into_storage_changes(
			&state,
			changes_trie_state.as_ref(),
			parent_hash,
		)?;

        Ok(BlockChanges {
            storage_changes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::test_harness;
    use polkadot_service::Block;

    #[test]
    fn should_create_new()
    {
        let (client, backend) = test_harness::client_backend("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        // let api = client.runtime_api();
        let executor = BlockExecutor::<Block, _, _>::new(&client, &backend);
    }
}
