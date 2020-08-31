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

//! Executor for calls into the runtime
//! A slimmed down changes-trie-disabled, offchain changes disabled, cache disabled, LocalExecutor

use codec::{Decode, Encode};
use futures::Future;
use sc_client_api::{backend, call_executor::CallExecutor};
use sc_executor::{NativeVersion, RuntimeInfo, RuntimeVersion};
use sp_api::{InitializeBlock, ProofRecorder, StorageTransactionCache};
use sp_core::{
    offchain::storage::OffchainOverlayedChanges,
    traits::{CodeExecutor, SpawnNamed},
    NativeOrEncoded, NeverNativeValue,
};
use sp_externalities::Extensions;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, HashFor, NumberFor},
};
use sp_state_machine::{
    self, ExecutionManager, ExecutionStrategy, Ext, OverlayedChanges, StateMachine, StorageProof,
};
use std::{cell::RefCell, panic::UnwindSafe, result, sync::Arc};

// SpawnNamed is not implemented for Arc<dyn SpawnNamed>
#[derive(Clone)]
struct SpawnWrapper(Arc<dyn SpawnNamed + Send + Sync>);

impl SpawnNamed for SpawnWrapper {
    fn spawn(
        &self,
        n: &'static str,
        fut: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) {
        self.0.spawn(n, fut)
    }

    fn spawn_blocking(
        &self,
        n: &'static str,
        fut: std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) {
        self.0.spawn_blocking(n, fut)
    }
}

/// Call executor that executes methods locally, querying all required
/// data from local backend.
pub struct ArchiveExecutor<B, E> {
    backend: Arc<B>,
    executor: E,
    spawn_handle: SpawnWrapper,
}

impl<B, E> ArchiveExecutor<B, E> {
    /// Creates new instance of local call executor.
    pub fn new(
        backend: Arc<B>,
        executor: E,
        spawn_handle: impl SpawnNamed + Send + Sync + 'static,
    ) -> Self {
        let spawn_handle = SpawnWrapper(Arc::new(spawn_handle));
        ArchiveExecutor {
            backend,
            executor,
            spawn_handle,
        }
    }
}

impl<B, E> Clone for ArchiveExecutor<B, E>
where
    E: Clone,
{
    fn clone(&self) -> Self {
        ArchiveExecutor {
            backend: self.backend.clone(),
            executor: self.executor.clone(),
            spawn_handle: self.spawn_handle.clone(),
        }
    }
}

impl<B, E, Block> CallExecutor<Block> for ArchiveExecutor<B, E>
where
    B: backend::Backend<Block>,
    E: CodeExecutor + RuntimeInfo + Clone + 'static,
    Block: BlockT,
{
    type Error = E::Error;

    type Backend = B;

    fn call(
        &self,
        id: &BlockId<Block>,
        method: &str,
        call_data: &[u8],
        strategy: ExecutionStrategy,
        extensions: Option<Extensions>,
    ) -> sp_blockchain::Result<Vec<u8>> {
        let mut changes = OverlayedChanges::default();
        let mut offchain_changes = OffchainOverlayedChanges::disabled();

        let state = self.backend.state_at(*id)?;
        let state_runtime_code = sp_state_machine::backend::BackendRuntimeCode::new(&state);
        // changes trie block number is not used, so we set it to u32
        // these types can be removed if changes-trie is decided to be used
        let return_data = StateMachine::<_, _, u32, _>::new(
            &state,
            None, // Changes Trie
            &mut changes,
            &mut offchain_changes,
            &self.executor,
            method,
            call_data,
            extensions.unwrap_or_default(),
            &state_runtime_code.runtime_code()?,
            self.spawn_handle.clone(),
        )
        .execute_using_consensus_failure_handler::<_, NeverNativeValue, fn() -> _>(
            strategy.get_manager(),
            None,
        )?;

        Ok(return_data.into_encoded())
    }

    fn contextual_call<
        'a,
        IB: Fn() -> sp_blockchain::Result<()>,
        EM: Fn(
            Result<NativeOrEncoded<R>, Self::Error>,
            Result<NativeOrEncoded<R>, Self::Error>,
        ) -> Result<NativeOrEncoded<R>, Self::Error>,
        R: Encode + Decode + PartialEq,
        NC: FnOnce() -> result::Result<R, String> + UnwindSafe,
    >(
        &self,
        initialize_block_fn: IB,
        at: &BlockId<Block>,
        method: &str,
        call_data: &[u8],
        changes: &RefCell<OverlayedChanges>,
        offchain_changes: &RefCell<OffchainOverlayedChanges>,
        _storage_transaction_cache: Option<
            &RefCell<
                StorageTransactionCache<Block, B::State>, // disabled
            >,
        >,
        initialize_block: InitializeBlock<'a, Block>,
        execution_manager: ExecutionManager<EM>,
        native_call: Option<NC>,
        _recorder: &Option<ProofRecorder<Block>>, // disabled
        extensions: Option<Extensions>,
    ) -> Result<NativeOrEncoded<R>, sp_blockchain::Error>
    where
        ExecutionManager<EM>: Clone,
    {
        match initialize_block {
            InitializeBlock::Do(ref init_block)
                if init_block
                    .borrow()
                    .as_ref()
                    .map(|id| id != at)
                    .unwrap_or(true) =>
            {
                initialize_block_fn()?;
            }
            // We don't need to initialize the runtime at a block.
            _ => {}
        }

        let state = self.backend.state_at(*at)?;

        let changes = &mut *changes.borrow_mut();
        let offchain_changes = &mut *offchain_changes.borrow_mut();

        let state_runtime_code = sp_state_machine::backend::BackendRuntimeCode::new(&state);
        let runtime_code = state_runtime_code.runtime_code()?;
        // changes trie block number is not used, so we set it to u32
        // these types can be removed if changes-trie is decided to be used
        let data = StateMachine::<_, _, u32, _>::new(
            &state,
            None, // changes trie state
            changes,
            offchain_changes,
            &self.executor,
            method,
            call_data,
            extensions.unwrap_or_default(),
            &runtime_code,
            self.spawn_handle.clone(),
        )
        .execute_using_consensus_failure_handler(execution_manager, native_call)?;
        Ok(data)
    }

    fn runtime_version(&self, id: &BlockId<Block>) -> sp_blockchain::Result<RuntimeVersion> {
        let mut overlay = OverlayedChanges::default();
        let mut offchain_overlay = OffchainOverlayedChanges::default();
        let state = self.backend.state_at(*id)?;
        let mut cache = StorageTransactionCache::<Block, B::State>::default();
        let mut ext = Ext::new(
            &mut overlay,
            &mut offchain_overlay,
            &mut cache,
            &state,
            None,
            None,
        );
        let state_runtime_code = sp_state_machine::backend::BackendRuntimeCode::new(&state);
        self.executor
            .runtime_version(&mut ext, &state_runtime_code.runtime_code()?)
            .map_err(|e| sp_blockchain::Error::VersionInvalid(format!("{:?}", e)))
    }

    fn prove_at_trie_state<S: sp_state_machine::TrieBackendStorage<HashFor<Block>>>(
        &self,
        trie_state: &sp_state_machine::TrieBackend<S, HashFor<Block>>,
        overlay: &mut OverlayedChanges,
        method: &str,
        call_data: &[u8],
    ) -> Result<(Vec<u8>, StorageProof), sp_blockchain::Error> {
        sp_state_machine::prove_execution_on_trie_backend::<_, _, NumberFor<Block>, _, _>(
            trie_state,
            overlay,
            &self.executor,
            self.spawn_handle.clone(),
            method,
            call_data,
            &sp_state_machine::backend::BackendRuntimeCode::new(trie_state).runtime_code()?,
        )
        .map_err(Into::into)
    }

    fn native_runtime_version(&self) -> Option<&NativeVersion> {
        Some(self.executor.native_version())
    }
}

impl<B, E, Block> sp_version::GetRuntimeVersion<Block> for ArchiveExecutor<B, E>
where
    B: backend::Backend<Block>,
    E: CodeExecutor + RuntimeInfo + Clone + 'static,
    Block: BlockT,
{
    fn native_version(&self) -> &sp_version::NativeVersion {
        self.executor.native_version()
    }

    fn runtime_version(&self, at: &BlockId<Block>) -> Result<sp_version::RuntimeVersion, String> {
        CallExecutor::runtime_version(self, at).map_err(|e| format!("{:?}", e))
    }
}
