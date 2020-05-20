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
mod database;
mod storage_backend;
mod util;

pub use self::{client::client, util::open_database};
pub use self::storage_backend::StorageBackend;
use sc_client_api::{backend::StorageProvider, client::BlockBackend};
use sp_blockchain::{HeaderBackend, HeaderMetadata, Error as BlockchainError};
use sp_runtime::traits::Block as BlockT;

/// Super trait defining what access the archive node needs to siphon data from running chains
pub trait ChainAccess<Block>:
StorageProvider<Block, sc_client_db::Backend<Block>>
    + BlockBackend<Block>
    + HeaderBackend<Block>
    + HeaderMetadata<Block, Error = BlockchainError>
where
    Block: BlockT,
{
}

impl<T, Block> ChainAccess<Block> for T
where
    Block: BlockT,
    T: StorageProvider<Block, sc_client_db::Backend<Block>>
    + BlockBackend<Block>
    + HeaderBackend<Block>
    + HeaderMetadata<Block, Error = BlockchainError>,
{
}
