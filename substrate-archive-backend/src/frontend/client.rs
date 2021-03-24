// Copyright 2017-2021 Parity Technologies (UK) Ltd.
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

use std::{marker::PhantomData, panic::UnwindSafe, sync::Arc};

use codec::{Decode, Encode};

use sc_client_api::{backend::Backend as _, execution_extensions::ExecutionExtensions, CallExecutor};
use sc_executor::RuntimeVersion;
use sp_api::{
	ApiError, ApiRef, CallApiAt, CallApiAtParams, ConstructRuntimeApi, Core as CoreApi, Metadata, ProvideRuntimeApi,
};
use sp_blockchain::HeaderBackend as _;
use sp_core::NativeOrEncoded;
use sp_runtime::{
	generic::BlockId,
	traits::{Block as BlockT, Header as HeaderT, One},
};
use sp_version::{GetRuntimeVersion, NativeVersion};

use crate::{
	database::ReadOnlyDB,
	error::BackendError,
	read_only_backend::{ReadOnlyBackend, TrieState},
};

// This trait allows circumvents putting <R, C> on an object that just needs to get the metadata
/// Trait to get the opaque metadata from the Runtime Api
pub trait GetMetadata<Block: BlockT>: Send + Sync {
	fn metadata(&self, id: &BlockId<Block>) -> Result<sp_core::OpaqueMetadata, BackendError>;
}

/// Archive Client
pub struct Client<Exec, Block: BlockT, RA, D: ReadOnlyDB> {
	backend: Arc<ReadOnlyBackend<Block, D>>,
	executor: Exec,
	execution_extensions: ExecutionExtensions<Block>,
	_marker: PhantomData<RA>,
}

impl<Exec, Block, RA, D> Client<Exec, Block, RA, D>
where
	D: ReadOnlyDB + 'static,
	Exec: CallExecutor<Block> + GetRuntimeVersion<Block> + Send + Sync,
	Block: BlockT,
	RA: Send + Sync,
{
	pub fn new(
		backend: Arc<ReadOnlyBackend<Block, D>>,
		executor: Exec,
		execution_extensions: ExecutionExtensions<Block>,
	) -> Result<Self, BackendError> {
		Ok(Client { backend, executor, execution_extensions, _marker: PhantomData })
	}

	pub fn state_at(&self, id: &BlockId<Block>) -> Option<TrieState<Block, D>> {
		self.backend.state_at(*id).ok()
	}

	pub fn runtime_version_at(&self, id: &BlockId<Block>) -> Result<RuntimeVersion, BackendError> {
		GetRuntimeVersion::runtime_version(self, id).map_err(Into::into)
	}

	pub fn native_runtime_version(&self) -> &NativeVersion {
		self.executor.native_version()
	}

	/// get the backend for this client instance
	pub fn backend(&self) -> Arc<ReadOnlyBackend<Block, D>> {
		self.backend.clone()
	}

	fn prepare_environment_block(&self, parent: &BlockId<Block>) -> sp_blockchain::Result<Block::Header> {
		let parent_header = self.backend.blockchain().expect_header(*parent)?;
		Ok(<<Block as BlockT>::Header as HeaderT>::new(
			self.backend.blockchain().expect_block_number_from_id(parent)? + One::one(),
			Default::default(),
			Default::default(),
			parent_header.hash(),
			Default::default(),
		))
	}
}

impl<Exec, Block, RA, D> GetRuntimeVersion<Block> for Client<Exec, Block, RA, D>
where
	D: ReadOnlyDB + 'static,
	Exec: CallExecutor<Block> + Send + Sync + GetRuntimeVersion<Block>,
	Block: BlockT,
	RA: Send + Sync,
{
	fn runtime_version(&self, at: &BlockId<Block>) -> Result<sp_version::RuntimeVersion, String> {
		GetRuntimeVersion::runtime_version(&self.executor, at)
	}

	fn native_version(&self) -> &NativeVersion {
		self.executor.native_version()
	}
}

impl<Exec, Block, RA, D> GetMetadata<Block> for Client<Exec, Block, RA, D>
where
	D: ReadOnlyDB + 'static,
	Exec: CallExecutor<Block, Backend = ReadOnlyBackend<Block, D>> + GetRuntimeVersion<Block> + Send + Sync,
	Block: BlockT,
	RA: ConstructRuntimeApi<Block, Self> + Send + Sync,
	RA::RuntimeApi: sp_api::Metadata<Block> + Send + Sync + 'static,
{
	fn metadata(&self, id: &BlockId<Block>) -> Result<sp_core::OpaqueMetadata, BackendError> {
		self.runtime_api().metadata(id).map_err(Into::into)
	}
}

impl<Exec, Block, RA, D> ProvideRuntimeApi<Block> for Client<Exec, Block, RA, D>
where
	D: ReadOnlyDB + 'static,
	Exec: CallExecutor<Block, Backend = ReadOnlyBackend<Block, D>> + GetRuntimeVersion<Block> + Send + Sync,
	Block: BlockT,
	RA: ConstructRuntimeApi<Block, Self> + Send + Sync,
{
	type Api = <RA as ConstructRuntimeApi<Block, Self>>::RuntimeApi;
	fn runtime_api(&self) -> ApiRef<'_, Self::Api> {
		RA::construct_runtime_api(self)
	}
}

impl<E, Block, RA, D> CallApiAt<Block> for Client<E, Block, RA, D>
where
	D: ReadOnlyDB + 'static,
	E: CallExecutor<Block, Backend = ReadOnlyBackend<Block, D>> + GetRuntimeVersion<Block> + Send + Sync,
	Block: BlockT,
	RA: Send + Sync,
{
	type StateBackend = TrieState<Block, D>;

	fn call_api_at<
		R: Encode + Decode + PartialEq,
		NC: FnOnce() -> std::result::Result<R, ApiError> + UnwindSafe,
		C: CoreApi<Block>,
	>(
		&self,
		params: CallApiAtParams<Block, C, NC, TrieState<Block, D>>,
	) -> Result<NativeOrEncoded<R>, ApiError> {
		let core_api = params.core_api;
		let at = params.at;

		let (manager, extensions) = self.execution_extensions.manager_and_extensions(at, params.context);

		self.executor
			.contextual_call::<_, fn(_, _) -> _, _, _>(
				|| {
					core_api
						.initialize_block(at, &self.prepare_environment_block(at)?)
						.map_err(sp_blockchain::Error::RuntimeApiError)
				},
				at,
				params.function,
				&params.arguments,
				params.overlayed_changes,
				Some(params.storage_transaction_cache),
				params.initialize_block,
				manager,
				params.native_call,
				params.recorder,
				Some(extensions),
			)
			.map_err(Into::into)
	}

	fn runtime_version_at(&self, at: &BlockId<Block>) -> Result<RuntimeVersion, ApiError> {
		self.runtime_version(at).map_err(|e| ApiError::Application(Box::new(BackendError::from(e))))
	}
}
