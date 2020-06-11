// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

// #![allow(warnings)]
#![forbid(unsafe_code)]

mod actors;
pub mod archive;
pub mod backend;
mod database;
mod error;
mod queries;
mod rpc;
mod simple_db;
mod types;
mod util;

pub use actors::ArchiveContext;
pub use archive::{Archive, ArchiveConfig};
pub use error::Error;
pub use types::{NotSignedBlock, Substrate};

#[cfg(feature = "logging")]
pub use util::init_logger;

// Re-Exports

pub use sp_blockchain::Error as BlockchainError;
pub use sp_core::twox_128;
pub use sp_storage::StorageKey;
pub mod chain_traits {
    //! Traits defining functions on the client needed for indexing
    pub use sc_client_api::{backend::StorageProvider, client::BlockBackend, UsageProvider};
    pub use sp_blockchain::{HeaderBackend, HeaderMetadata};
    pub use sp_runtime::traits::{BlakeTwo256, Block};
}
