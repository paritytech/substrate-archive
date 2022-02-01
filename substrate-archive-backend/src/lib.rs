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

//! Read Only Interface with Substrate Backend (kvdb-rocksdb)
//! Structs and traits in this module generally follow those defined in substrate.
//! They only differentiate insofar as the use of a read-only Secondary RocksDB Database.
//! Therefore some methods (like block importing) are not useful to us and left as stubs (generally with a warn! log).
//! We need to implement substrate database traits in order to satisfy the requirements for using a client.
//! We need a client implementation in order to access functions in a FRAME runtime (like block execution)
//! to allow for faster indexing.
//! These traits are:
//! [`Backend`](https://paritytech.github.io/substrate/master/sc_client_api/backend/trait.Backend.html)
//! [`BlockBackend`](https://paritytech.github.io/substrate/master/sp_blockchain/trait.Backend.html)
//! [`AuxStore`](https://paritytech.github.io/substrate/master/sc_client_api/backend/trait.AuxStore.html)
//! [`OffchainStorage`](https://paritytech.github.io/substrate/master/sp_core/offchain/trait.OffchainStorage.html)
//! [`BlockImportOperation`](https://paritytech.github.io/substrate/master/sc_client_db/struct.BlockImportOperation.html)
//!
//! the trait [`StateBackend`](https://paritytech.github.io/substrate/master/sc_client_api/backend/trait.StateBackend.html)
//! is implemented on a trimmed-down [`TrieState`] type and used in the archive client.

#![forbid(unsafe_code)]
#![deny(dead_code)]

mod database;
mod error;
mod frontend;
mod read_only_backend;
mod runtime_version_cache;
mod util;

use std::sync::Arc;

use sc_client_api::Backend as BackendT;
use sp_api::{CallApiAt, ConstructRuntimeApi, ProvideRuntimeApi};
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};
use sp_version::GetRuntimeVersionAt;

use self::frontend::GetMetadata;
// re-exports
pub use self::{
	database::{KeyValuePair, ReadOnlyDb, SecondaryRocksDb},
	error::BackendError,
	frontend::{runtime_api, ExecutionMethod, RuntimeConfig, TArchiveClient},
	read_only_backend::ReadOnlyBackend,
	runtime_version_cache::RuntimeVersionCache,
};

pub type Meta<B> = Arc<dyn GetMetadata<B>>;

/// supertrait for accessing methods that rely on internal runtime api
pub trait ApiAccess<Block, Backend, Runtime>:
	ProvideRuntimeApi<Block, Api = Runtime::RuntimeApi>
	+ Sized
	+ Send
	+ Sync
	+ CallApiAt<Block, StateBackend = Backend::State>
	+ GetMetadata<Block>
	+ GetRuntimeVersionAt<Block>
where
	Block: BlockT,
	Backend: BackendT<Block>,
	Runtime: ConstructRuntimeApi<Block, Self>,
{
}

impl<Client, Block, Backend, Runtime> ApiAccess<Block, Backend, Runtime> for Client
where
	Block: BlockT,
	Runtime: ConstructRuntimeApi<Block, Self>,
	Backend: BackendT<Block>,
	Client: ProvideRuntimeApi<Block, Api = Runtime::RuntimeApi>
		+ CallApiAt<Block, StateBackend = Backend::State>
		+ GetMetadata<Block>
		+ GetRuntimeVersionAt<Block>
		+ Sized
		+ Send
		+ Sync,
{
}

/// A set of APIs that runtimes must implement in order to be compatible with substrate-archive.
pub trait RuntimeApiCollection<Block>: sp_api::ApiExt<Block>
where
	Block: BlockT,
	<Self as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
}

impl<Api, Block> RuntimeApiCollection<Block> for Api
where
	Block: BlockT,
	Api: sp_api::ApiExt<Block>,
	<Self as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
}
