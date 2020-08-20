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

//! Read Only Interface with Substrate Backend (kvdb-rocksdb)

mod block_exec;
mod database;
pub mod frontend;
mod read_only_backend;
mod runtime_version_cache;
// #[cfg(test)]
// pub mod test_util;
pub mod util;

// re-exports
pub use self::block_exec::{BlockChanges, BlockExecutor};
pub use self::frontend::{GetMetadata, GetRuntimeVersion, TArchiveClient};
pub use self::read_only_backend::{ReadOnlyBackend, TrieState};
pub use self::runtime_version_cache::{RuntimeVersionCache, VersionRange};
pub use self::{database::ReadOnlyDatabase, frontend::runtime_api, util::open_database};

use sc_client_api::Backend as BackendT;
use sp_api::{CallApiAt, ConstructRuntimeApi, ProvideRuntimeApi};
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};
use std::sync::Arc;

pub type Meta<B> = Arc<dyn GetMetadata<B>>;

/// supertrait for accessing methods that rely on internal runtime api
pub trait ApiAccess<Block, Backend, Runtime>:
    ProvideRuntimeApi<Block, Api = Runtime::RuntimeApi>
    + Sized
    + Send
    + Sync
    + CallApiAt<Block, Error = sp_blockchain::Error, StateBackend = Backend::State>
    + GetMetadata<Block>
    + GetRuntimeVersion<Block>
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
        + CallApiAt<Block, Error = sp_blockchain::Error, StateBackend = Backend::State>
        + GetMetadata<Block>
        + GetRuntimeVersion<Block>
        + Sized
        + Send
        + Sync,
{
}

/// A set of APIs that runtimes must implement in order to be compatible with substrate-archive.
pub trait RuntimeApiCollection<Block>: sp_api::ApiExt<Block, Error = sp_blockchain::Error>
where
    Block: BlockT,
    <Self as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
}

impl<Api, Block> RuntimeApiCollection<Block> for Api
where
    Block: BlockT,
    Api: sp_api::ApiExt<Block, Error = sp_blockchain::Error>,
    <Self as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
{
}
