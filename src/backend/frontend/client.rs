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

//! Custom implementation of a Client for Archival use
//! Implements Runtime API's
//! Read-Only Access
//! The Archive Client and it's backend are decoupled
//! It's recommended to use the backend (ReadOnlyBackend) for anything that requires getting blocks, querying
//! storage, or similar operations. Client usage should be reserved for calling into the Runtime

use crate::{
    backend::{ReadOnlyBackend, TrieState},
    error::Result,
    Error,
};
use codec::{Decode, Encode};
use sc_client_api::{
    backend::Backend as _, execution_extensions::ExecutionExtensions, CallExecutor,
};
use sc_executor::RuntimeVersion;
use sp_api::{
    ApiRef, CallApiAt, CallApiAtParams, ConstructRuntimeApi, Core as CoreApi, Metadata,
    ProvideRuntimeApi,
};
use sp_blockchain::HeaderBackend as _;
use sp_core::NativeOrEncoded;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header as HeaderT, One},
};
use std::{marker::PhantomData, panic::UnwindSafe, sync::Arc};

// FIXME: should use the trait sp_version::GetRuntimeVersion
// but that returns a String for an error
pub trait GetRuntimeVersion<Block: BlockT>: Send + Sync {
    fn runtime_version(&self, at: &BlockId<Block>) -> Result<sp_version::RuntimeVersion>;
}

// This trait allows circumvents putting <R, C> on an object that just needs to get the metadata
/// Trait to get the opaque metadata from the Runtime Api
pub trait GetMetadata<Block: BlockT>: Send + Sync {
    fn metadata(&self, id: &BlockId<Block>) -> Result<sp_core::OpaqueMetadata>;
}

/// Archive Client
pub struct Client<Exec, Block: BlockT, RA> {
    backend: Arc<ReadOnlyBackend<Block>>,
    executor: Exec,
    execution_extensions: ExecutionExtensions<Block>,
    _marker: PhantomData<RA>,
}

impl<Exec, Block, RA> Client<Exec, Block, RA>
where
    Exec: CallExecutor<Block>,
    Block: BlockT,
{
    pub fn new(
        backend: Arc<ReadOnlyBackend<Block>>,
        executor: Exec,
        execution_extensions: ExecutionExtensions<Block>,
    ) -> Result<Self> {
        Ok(Client {
            backend,
            executor,
            execution_extensions,
            _marker: PhantomData,
        })
    }

    pub fn state_at(&self, id: &BlockId<Block>) -> Option<TrieState<Block>> {
        self.backend.state_at(*id).ok()
    }

    pub fn runtime_version_at(&self, id: &BlockId<Block>) -> Result<RuntimeVersion> {
        self.executor.runtime_version(id).map_err(Error::from)
    }

    /// get the backend for this client instance
    pub fn backend(&self) -> Arc<ReadOnlyBackend<Block>> {
        self.backend.clone()
    }

    fn prepare_environment_block(
        &self,
        parent: &BlockId<Block>,
    ) -> sp_blockchain::Result<Block::Header> {
        let parent_header = self.backend.blockchain().expect_header(*parent)?;
        Ok(<<Block as BlockT>::Header as HeaderT>::new(
            self.backend
                .blockchain()
                .expect_block_number_from_id(parent)?
                + One::one(),
            Default::default(),
            Default::default(),
            parent_header.hash(),
            Default::default(),
        ))
    }
}

impl<Exec, Block, RA> GetRuntimeVersion<Block> for Client<Exec, Block, RA>
where
    Exec: CallExecutor<Block, Backend = ReadOnlyBackend<Block>> + Send + Sync,
    Block: BlockT,
    RA: Send + Sync,
{
    fn runtime_version(&self, at: &BlockId<Block>) -> Result<sp_version::RuntimeVersion> {
        self.runtime_version_at(at)
    }
}

impl<Exec, Block, RA> GetMetadata<Block> for Client<Exec, Block, RA>
where
    Exec: CallExecutor<Block, Backend = ReadOnlyBackend<Block>> + Send + Sync,
    Block: BlockT,
    RA: ConstructRuntimeApi<Block, Self> + Send + Sync,
    RA::RuntimeApi: sp_api::Metadata<Block, Error = sp_blockchain::Error> + Send + Sync + 'static,
{
    fn metadata(&self, id: &BlockId<Block>) -> Result<sp_core::OpaqueMetadata> {
        self.runtime_api().metadata(id).map_err(Into::into)
    }
}

impl<Exec, Block, RA> ProvideRuntimeApi<Block> for Client<Exec, Block, RA>
where
    Exec: CallExecutor<Block, Backend = ReadOnlyBackend<Block>> + Send + Sync,
    Block: BlockT,
    RA: ConstructRuntimeApi<Block, Self>,
{
    type Api = <RA as ConstructRuntimeApi<Block, Self>>::RuntimeApi;
    fn runtime_api(&self) -> ApiRef<'_, Self::Api> {
        RA::construct_runtime_api(self)
    }
}

impl<E, Block, RA> CallApiAt<Block> for Client<E, Block, RA>
where
    E: CallExecutor<Block, Backend = ReadOnlyBackend<Block>> + Send + Sync,
    Block: BlockT,
{
    type Error = sp_blockchain::Error;
    type StateBackend = TrieState<Block>;

    fn call_api_at<
        'a,
        R: Encode + Decode + PartialEq,
        NC: FnOnce() -> std::result::Result<R, String> + UnwindSafe,
        C: CoreApi<Block, Error = sp_blockchain::Error>,
    >(
        &self,
        params: CallApiAtParams<'a, Block, C, NC, TrieState<Block>>,
    ) -> sp_blockchain::Result<NativeOrEncoded<R>> {
        let core_api = params.core_api;
        let at = params.at;

        let (manager, extensions) = self
            .execution_extensions
            .manager_and_extensions(at, params.context);

        self.executor.contextual_call::<_, fn(_, _) -> _, _, _>(
            || core_api.initialize_block(at, &self.prepare_environment_block(at)?),
            at,
            params.function,
            &params.arguments,
            params.overlayed_changes,
            params.offchain_changes,
            Some(params.storage_transaction_cache),
            params.initialize_block,
            manager,
            params.native_call,
            params.recorder,
            Some(extensions),
        )
    }

    fn runtime_version_at(&self, at: &BlockId<Block>) -> sp_blockchain::Result<RuntimeVersion> {
        self.runtime_version_at(at)
            .map_err(|e| sp_blockchain::Error::Msg(e.to_string()))
    }
}
