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
    error::Error as ArchiveError,
    types::*
};
use std::sync::Arc;
use sc_client_api::backend;
use sp_api::{Core, ApiExt, ApiRef, ProvideRuntimeApi, StorageChanges};
use sp_core::ExecutionContext;
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, HashFor, Header}
};

/// Storage Changes that occur as a result of a block's executions
pub struct BlockChanges<Block: BlockT, StateBackend: backend::StateBackend<HashFor<Block>>> {
	/// The changes that need to be applied to the backend to get the state of the build block.
	pub storage_changes: StorageChanges<StateBackend, Block>,
}
/*
pub struct BlockExecutor<'a, Block: BlockT, Api: ProvideRuntimeApi<Block>, B> {
    api: ApiRef<'a, Api::Api>,
    backend: Arc<B>,
    block: &'a Block,
    id: BlockId<Block>
}
*/

pub struct BlockExecutor<'a, Block, Api, B> 
where
    Block: BlockT,
    Api: BlockBuilderApi<Block, Error = sp_blockchain::Error>
        + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>
{
    api: ApiRef<'a, Api>,
    backend: Arc<B>,
    block: &'a Block,
    id: BlockId<Block>
}

impl<'a, Block, Api, B> BlockExecutor<'a, Block, Api, B>
where
    Block: BlockT,
    // Api: ProvideRuntimeApi<Block>,
    Api: BlockBuilderApi<Block, Error = sp_blockchain::Error> 
        + ApiExt<Block, StateBackend = backend::StateBackendFor<B, Block>>,
    B: backend::Backend<Block>,
{
    pub fn new(api: ApiRef<'a, Api>, backend: Arc<B>, block: &'a Block) -> Result<Self, ArchiveError> {
        // let api = api.runtime_api();
        let header = block.header();
        let parent_hash = header.parent_hash();
        let id = BlockId::Hash(*parent_hash); 
        api.initialize_block_with_context(&id, ExecutionContext::BlockConstruction, &header)?;

        Ok(Self {
            api, backend, block, id,
        })
    }

    fn push_all_ext(&self) -> Result<(), ArchiveError> {
        let block_id = &self.id;
        for ext in self.block.extrinsics().iter() {
            self.api.map_api_result(|api| {
                match api.apply_extrinsic_with_context(
                    block_id,
                    ExecutionContext::BlockConstruction,
                    ext.clone()
                )? {
                    Ok(_) => {
                        Ok(())
                    }
                    // transactions should never be invalid because we are applying extrinsics
                    // from already-finalized blocks
                    Err(tx_validity) => Err(ArchiveError::from("Invalid Transaction"))
                }
            });
        }
        Ok(())
    }

    pub fn block_into_storage(&self) -> Result<BlockChanges<Block, backend::StateBackendFor<B, Block>>, ArchiveError> {
        // we don't really want to finalize blocks, we just want storage changes 
        // let header = self.api.finalize_block_with_context(&self.id, ExecutionContext::BlockConstruction)?;
        // self.push_all_ext(); 
        let parent_hash = self.block.header().parent_hash();
        let state = self.backend.state_at(self.id)?;

		let storage_changes = self.api.into_storage_changes(
            &state,
            None,
            *parent_hash,
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
    use sp_blockchain::HeaderBackend;
    //use sp_runtime::traits::BlockBackend;
    use sc_client_api::BlockBackend;

    #[test]
    fn should_create_new() {
        let full_client = test_harness::client("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        let id = BlockId::Number(50970);
        // let header = full_client.header(id).unwrap().expect("No such block exists!");
        let block = full_client.block(&id).unwrap().expect("No such block exists!").block;
        let (client, backend) = test_harness::client_backend("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        let executor = BlockExecutor::new(&client, &backend, &block);
    }

    #[test]
    fn should_execute_blocks() {
        let full_client = test_harness::client("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        let id = BlockId::Number(50970);
        let block = full_client.block(&id).unwrap().expect("No such block exists!").block;
        let (client, backend) = test_harness::client_backend("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        let time = std::time::Instant::now();
        let executor = BlockExecutor::new(&client, &backend, &block).unwrap();
        let result = executor.block_into_storage().unwrap();
        let elapsed = time.elapsed();
        let main_changes = result.storage_changes.main_storage_changes;
        
        for (key, value) in main_changes.iter() {
            let key = hex::encode(key);
           
            if value.is_some() {
                let val = hex::encode(value.as_ref().unwrap()); 
                println!("Key: {}, Value: {}", key, val);
            } else {
                println!("Key: {}, Value: None", key);
            }
        }
        println!("Took {} seconds, {} milli-seconds, {} micro-seconds, {} nano-sconds", elapsed.as_secs(), elapsed.as_millis(), elapsed.as_micros(), elapsed.as_nanos());
    }

    #[test]
    fn should_not_keep_old_extrinsics() {
        let full_client = test_harness::client("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        let id0 = BlockId::Number(1000);
        let id1 = BlockId::Number(3000);
        let block0 = full_client.block(&id0).unwrap().expect("No such block exists!").block;
        let block1 = full_client.block(&id1).unwrap().expect("No such block exists!").block;
     
        let (client, backend) = test_harness::client_backend("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
        let time = std::time::Instant::now();
        let executor = BlockExecutor::new(&client, &backend, &block0).unwrap();
        let storage_changes0_0 = executor.block_into_storage().unwrap().storage_changes;

        let executor = BlockExecutor::new(&client, &backend, &block1).unwrap();
        let storage_changes1 = executor.block_into_storage().unwrap();
        
        let executor = BlockExecutor::new(&client, &backend, &block0).unwrap();
        let storage_changes0_1 = executor.block_into_storage().unwrap().storage_changes;
        let elapsed = time.elapsed();
        println!("Took {} seconds, {} milli-seconds, {} micro-seconds, {} nano-sconds", elapsed.as_secs(), elapsed.as_millis(), elapsed.as_micros(), elapsed.as_nanos());
        assert_eq!(storage_changes0_0.main_storage_changes, storage_changes0_1.main_storage_changes);
    }
}
