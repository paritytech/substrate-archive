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

mod client;
//mod api;
mod database;
mod storage_backend;
mod util;

pub use self::storage_backend::StorageBackend;
pub use self::{client::client, util::open_database};
use sp_api::{ProvideRuntimeApi, ConstructRuntimeApi, CallApiAt};
use sc_client_api::{backend::StorageProvider, client::BlockBackend, UsageProvider};
use sp_blockchain::{Error as BlockchainError, HeaderBackend, HeaderMetadata};
use sp_runtime::traits::{BlakeTwo256, Block as BlockT};
use sc_client_api::{Backend as BackendT, BlockchainEvents};

// Could make this supertrait accept a Executor and a RuntimeAPI generic arguments
// however, that would clutter the API when using ChainAccess everywhere within substrate-archive
// relying on the RPC for this is OK (for now)
/// Super trait defining what access the archive node needs to siphon data from running chains
pub trait ChainAccess<Block: BlockT>:
    StorageProvider<Block, sc_client_db::Backend<Block>>
    + BlockBackend<Block>
    + HeaderBackend<Block>
    + HeaderMetadata<Block, Error = BlockchainError>
    + UsageProvider<Block>
    + ProvideRuntimeApi<Block>
{
}

impl<T, Block> ChainAccess<Block> for T
where
    Block: BlockT,
    T: StorageProvider<Block, sc_client_db::Backend<Block>>
        + BlockBackend<Block>
        + HeaderBackend<Block>
        + HeaderMetadata<Block, Error = BlockchainError>
        + UsageProvider<Block>
        + ProvideRuntimeApi<Block>
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

// pub trait RuntimeExtrinsic: codec::Codec + Send + Sync + 'static {}

// impl<E> RuntimeExtrinsic for E where E: codec::Codec + Send + Sync + 'static {}
