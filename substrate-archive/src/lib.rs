// Copyright 2018-2021 Parity Technologies (UK) Ltd.
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

#![forbid(unsafe_code)]
#![deny(dead_code)]

//! ### Overview
//!
//! Archive is written as a way to index generic substrate chains into a uniform Postgres database for
//! the purpose of querying information about a network in a way most familiar to developers.
//! Therefore the core of archive is a library, and needs another layer to tell it about a networks Runtime
//! characteristics in order to index properly.
//!
//! #### Setup
//! The simplest possible setup for archive is described in the `simple` example [`examples/simple.rs`].
//! Most important is passing the correct `RuntimeApi`, `Block`, and `ReadOnlyDb` trait generics to the builder.
//! A more complicated setup may be observed in the `polkadot-archive` and `trex-archive` binary projects.
//! The trickiest part is setting up the Postgres and RabbitMq seperately and configuring those services.
//! A [Docker Compose
//! Config](https://github.com/paritytech/substrate-archive/blob/master/docker-compose.yaml) exists
//! to make this process easier.

// Re-Exports
pub use sp_blockchain::Error as BlockchainError;
pub use sp_runtime::MultiSignature;
pub use substrate_archive_backend::{ExecutionMethod, ReadOnlyDb, RuntimeConfig, SecondaryRocksDb};

mod actors;
pub mod archive;
pub mod database;
mod error;
mod logger;
mod tasks;
mod types;
mod wasm_tracing;

pub use self::actors::{ControlConfig, System};
pub use self::archive::{Archive, ArchiveBuilder, ArchiveConfig, ChainConfig, TracingConfig};
pub use self::database::{queries, DatabaseConfig};
pub use self::error::ArchiveError;

pub mod chain_traits {
	//! Traits defining functions on the client needed for indexing
	pub use sc_client_api::client::BlockBackend;
	pub use sp_blockchain::{HeaderBackend, HeaderMetadata};
	pub use sp_runtime::traits::{BlakeTwo256, Block, IdentifyAccount, Verify};
}

/// Get the path to a local substrate directory where we can save data.
/// Platform | Value | Example
/// -- | -- | --
/// Linux | $XDG_DATA_HOME or $HOME/.local/share/substrate_archive | /home/alice/.local/share/substrate_archive/
/// macOS | $HOME/Library/Application Support/substrate_archive | /Users/Alice/Library/Application Support/substrate_archive/
/// Windows | {FOLDERID_LocalAppData}\substrate_archive | C:\Users\Alice\AppData\Local\substrate_archive
pub fn substrate_archive_default_dir() -> std::path::PathBuf {
	let base_dirs = dirs::BaseDirs::new().expect("Invalid home directory path");
	let mut path = base_dirs.data_local_dir().to_path_buf();
	path.push("substrate_archive");
	path
}

#[cfg(test)]
pub use test::*;

#[cfg(test)]
mod test {
	use crate::database::BlockModel;
	use async_std::task;
	use std::sync::Once;
	use test_common::{CsvBlock, DATABASE_URL};

	static INIT: Once = Once::new();
	/// Guard that should be called at the beginning of every test.
	pub fn initialize() {
		INIT.call_once(|| {
			pretty_env_logger::init();
			let url: &str = &DATABASE_URL;
			task::block_on(async {
				crate::database::setup(url, Default::default(), vec![]).await.unwrap();
			});
		});
	}

	impl From<test_common::CsvBlock> for BlockModel {
		fn from(csv: CsvBlock) -> BlockModel {
			BlockModel {
				id: csv.id,
				parent_hash: csv.parent_hash,
				hash: csv.hash,
				block_num: csv.block_num,
				state_root: csv.state_root,
				extrinsics_root: csv.extrinsics_root,
				digest: csv.digest,
				ext: csv.ext,
				spec: csv.spec,
			}
		}
	}
}
