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

//! Implements the main backend trait for ReadOnlyBackend struct

use super::misc_backend::{OffchainStorageBackend, RealBlockImportOperation};
use super::ReadOnlyBackend;
use sc_client_api::{
    backend::{Backend, PrunableStateChangesTrieStorage},
    client::UsageInfo,
};
use sp_blockchain::{Error as BlockchainError, HeaderBackend as _};
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, NumberFor},
    Justification,
};

type ChainResult<T> = Result<T, BlockchainError>;

impl<Block: BlockT> Backend<Block> for ReadOnlyBackend<Block> {
    type BlockImportOperation = RealBlockImportOperation;
    type OffchainStorage = OffchainStorageBackend;
    type Blockchain = Self;
    type State = super::state_backend::TrieState<Block>;

    fn begin_operation(&self) -> ChainResult<Self::BlockImportOperation> {
        log::warn!("Block import operations are not supported for Read Only Backend");
        Ok(RealBlockImportOperation)
    }

    fn begin_state_operation(
        &self,
        _operation: &mut Self::BlockImportOperation,
        _block: BlockId<Block>,
    ) -> ChainResult<()> {
        log::warn!("State operations not supported on a read-only backend. Operation not begun.");
        Ok(())
    }

    fn commit_operation(&self, _transaction: Self::BlockImportOperation) -> ChainResult<()> {
        log::warn!(
            "State operations not supported on a read-only backend. Operation not committed"
        );
        Ok(())
    }

    fn finalize_block(
        &self,
        _block: BlockId<Block>,
        _justification: Option<Justification>,
    ) -> ChainResult<()> {
        log::warn!("State operations not supported on a read-only backend. Block not finalized.");
        Ok(())
    }

    fn blockchain(&self) -> &Self::Blockchain {
        self
    }

    fn usage_info(&self) -> Option<UsageInfo> {
        // TODO: Implement usage info (for state reads)
        log::warn!("Usage info not supported");
        None
    }

    fn changes_trie_storage(&self) -> Option<&dyn PrunableStateChangesTrieStorage<Block>> {
        // TODO: Implement Changes Trie
        // log::warn!("Changes trie not supported");
        None
    }

    fn offchain_storage(&self) -> Option<Self::OffchainStorage> {
        log::warn!("Offchain Storage not supported");
        None
    }

    fn state_at(&self, block: BlockId<Block>) -> ChainResult<Self::State> {
        let hash = match block {
            BlockId::Number(n) => {
                let h = self.hash(n)?;
                if h.is_none() {
                    return Err(BlockchainError::UnknownBlock(format!(
                        "No block found for {:?}",
                        n
                    )));
                } else {
                    h.expect("Checked for some; qed")
                }
            }
            BlockId::Hash(h) => h,
        };

        match self.state_at(hash) {
            Some(v) => Ok(v),
            None => Err(BlockchainError::Msg(format!(
                "No state found for block {:?}",
                hash
            ))),
        }
    }

    fn revert(
        &self,
        _n: NumberFor<Block>,
        _revert_finalized: bool,
    ) -> ChainResult<(NumberFor<Block>, std::collections::HashSet<Block::Hash>)> {
        log::warn!("Reverting blocks not supported for a read only backend");
        Err(BlockchainError::Msg(
            "Reverting blocks not supported".into(),
        ))
    }

    fn get_import_lock(&self) -> &parking_lot::RwLock<()> {
        panic!("No lock exists for read only backend!")
    }
}
