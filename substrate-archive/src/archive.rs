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

use std::{env, fs, marker::PhantomData, path::PathBuf, sync::Arc};

use serde::de::DeserializeOwned;

use sc_chain_spec::ChainSpec;
use sc_client_api::backend as api_backend;
use sc_executor::NativeExecutionDispatch;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_blockchain::{Backend as BlockchainBackend, Error as BlockchainError};
use sp_runtime::{
	generic::BlockId,
	traits::{BlakeTwo256, Block as BlockT, NumberFor},
};

use substrate_archive_backend::{runtime_api, ReadOnlyBackend, TArchiveClient};
use substrate_archive_common::{util, ReadOnlyDB, Result};

use crate::{
	actors::{ActorContext, System},
	migrations,
};

const CHAIN_DATA_VAR: &str = "CHAIN_DATA_DB";
const POSTGRES_VAR: &str = "DATABASE_URL";

/// The recommended open file descriptor limit to be configured for the process.
const RECOMMENDED_OPEN_FILE_DESCRIPTOR_LIMIT: u64 = 10_000;

/// The control interface of an archive system.
#[async_trait::async_trait(?Send)]
pub trait Archive<B: BlockT + Unpin, D: ReadOnlyDB>
where
	B::Hash: Unpin,
{
	/// start driving the execution of the archive
	fn drive(&mut self) -> Result<()>;

	/// this method will block indefinitely
	async fn block_until_stopped(&self) -> ();

	/// shutdown the system
	fn shutdown(self) -> Result<()>;

	/// Shutdown the system when self is boxed (useful when erasing the types of the runtime)
	fn boxed_shutdown(self: Box<Self>) -> Result<()>;

	/// Get a reference to the context the actors are using
	fn context(&self) -> Result<ActorContext<B, D>>;
}

pub struct ArchiveBuilder<B, R, D, DB> {
	_marker: PhantomData<(B, R, D, DB)>,
	/// Chain spec describing the chain
	chain_spec: Option<Box<dyn ChainSpec>>,
	/// Path to the rocksdb database
	chain_data_path: Option<PathBuf>,
	/// url to the Postgres Database
	pg_url: Option<String>,
	/// how much Cache should Rocksdb Keep
	cache_size: usize,
	/// number of threads to spawn for block execution
	block_workers: usize,
	/// Number of 64KB Heap pages to allocate for wasm execution
	wasm_pages: u64,
	/// maximum amount of blocks to index at once
	max_block_load: u32,
}

impl<B, R, D, DB> Default for ArchiveBuilder<B, R, D, DB> {
	fn default() -> Self {
		let num_cpus = num_cpus::get();
		Self {
			_marker: PhantomData,
			chain_spec: None,
			chain_data_path: None,
			pg_url: None,
			cache_size: 128,                  // 128 MB
			block_workers: num_cpus,          // the number of logical cpus in the system
			wasm_pages: 64 * num_cpus as u64, // 64 * (number of logic cpu's)
			max_block_load: 100_000,          // 100_000 blocks to index at once
		}
	}
}

impl<B, R, D, DB> ArchiveBuilder<B, R, D, DB> {
	/// Specify a chain spec for storing metadata about the running archiver
	/// in a persistant directory.
	///
	/// # Default
	/// Defaults to storing metadata in a temporary directory.
	pub fn chain_spec(mut self, spec: Box<dyn ChainSpec>) -> Self {
		self.chain_spec = Some(spec);
		self
	}

	/// Set the chain data backend path to use for this instance.
	///
	/// # Default
	/// defaults to the environment variable CHAIN_DATA_DB
	pub fn chain_data_path<S: Into<PathBuf>>(mut self, path: Option<S>) -> Self {
		self.chain_data_path = path.map(Into::into);
		self
	}

	/// Set the url to the Postgres Database
	///
	/// # Default
	/// defaults to value of the environment variable DATABASE_URL
	pub fn pg_url<S: Into<String>>(mut self, url: Option<S>) -> Self {
		self.pg_url = url.map(Into::into);
		self
	}

	/// Set the amount of cache Rocksdb should keep.
	///
	/// # Default
	/// defaults to 128MB
	pub fn cache_size(mut self, cache_size: Option<usize>) -> Self {
		if let Some(cache_size) = cache_size {
			self.cache_size = cache_size;
		}
		self
	}

	/// Set the number of threads spawn for block execution.
	///
	/// # Default
	/// defaults to the number of logical cpus in the system
	pub fn block_workers(mut self, workers: Option<usize>) -> Self {
		if let Some(block_workers) = workers {
			self.block_workers = block_workers;
		}
		self
	}

	/// Number of 64KB Heap Pages to allocate for WASM execution
	///
	/// # Default
	/// defaults to 64 * (number of logic cpu's)
	pub fn wasm_pages(mut self, pages: Option<u64>) -> Self {
		if let Some(wasm_pages) = pages {
			self.wasm_pages = wasm_pages;
		}
		self
	}

	/// Set the number of blocks to index at once
	///
	/// # Default
	/// Defaults to 100_000
	pub fn max_block_load(mut self, max_block_load: Option<u32>) -> Self {
		if let Some(max_block_load) = max_block_load {
			self.max_block_load = max_block_load;
		}
		self
	}
}

/// Create rocksdb secondary directory if it doesn't exist yet.
/// If the ChainSpec is not specified, a temporary directory is used.
/// Return path to that directory
///
/// # Panics
///
/// Panics if the directories fail to be created.
fn create_database_path(spec: Option<Box<dyn ChainSpec>>) -> Result<PathBuf> {
	let path = if let Some(spec) = spec {
		let (chain, id) = (spec.name(), spec.id());
		let mut path = util::substrate_dir()?;
		path.extend(&["rocksdb_secondary", chain, id]);
		fs::create_dir_all(path.as_path())?;
		path
	} else {
		// TODO: make sure this is cleaned up on kill
		tempfile::tempdir()?.into_path()
	};

	Ok(path)
}

impl<B, R, D, DB> ArchiveBuilder<B, R, D, DB>
where
	DB: ReadOnlyDB + 'static,
	B: BlockT + Unpin + DeserializeOwned,
	R: ConstructRuntimeApi<B, TArchiveClient<B, R, D, DB>> + Send + Sync + 'static,
	R::RuntimeApi: BlockBuilderApi<B, Error = BlockchainError>
		+ sp_api::Metadata<B, Error = BlockchainError>
		+ ApiExt<B, StateBackend = api_backend::StateBackendFor<ReadOnlyBackend<B, DB>, B>>
		+ Send
		+ Sync
		+ 'static,
	D: NativeExecutionDispatch + 'static,
	<R::RuntimeApi as sp_api::ApiExt<B>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
	NumberFor<B>: Into<u32> + From<u32> + Unpin,
	B::Hash: Unpin,
	B::Header: serde::de::DeserializeOwned,
{
	/// Build this instance of the Archiver.
	/// Runs the database migrations for the database at `pg_url`.
	///
	/// # Panics
	/// Panics if one of chain_data_db or pg_url is not passed to the builder
	/// and their respective environment variables are not set.
	pub fn build(self) -> Result<impl Archive<B, DB>> {
		let chain_path = self.chain_data_path.unwrap_or_else(|| {
			env::var(CHAIN_DATA_VAR).expect("CHAIN_DATA_DB must be set if not passed initially.").into()
		});
		let chain_path = chain_path.to_str().expect("chain data path is invalid");
		let db_path = create_database_path(self.chain_spec)?;
		let db = Arc::new(DB::open_database(chain_path, self.cache_size, db_path)?);

		let pg_url = self
			.pg_url
			.unwrap_or_else(|| env::var(POSTGRES_VAR).expect("DATABASE_URL must be set if not passed initially."));
		smol::block_on(migrations::migrate(&pg_url))?;

		let client = Arc::new(runtime_api::<B, R, D, DB>(db.clone(), self.block_workers, self.wasm_pages)?);
		let backend = Arc::new(ReadOnlyBackend::new(db, true));
		Self::startup_info(&*client, &*backend)?;

		let ctx = System::<_, R, _, _>::new(client, backend, self.block_workers, pg_url.as_str(), self.max_block_load)?;
		Ok(ctx)
	}

	/// Log some general startup info
	fn startup_info(client: &TArchiveClient<B, R, D, DB>, backend: &ReadOnlyBackend<B, DB>) -> Result<()> {
		let last_finalized_block = backend.last_finalized()?;
		let rt = client.runtime_version_at(&BlockId::Hash(last_finalized_block))?;
		log::info!(
            "Running archive for chain `{}` 🔗, implementation `{}`. Latest known runtime version: {}. Latest finalized block {} 🛡️",
            rt.spec_name,
            rt.impl_name,
            rt.spec_version,
            last_finalized_block
        );
		if let Some(new_limit) = fdlimit::raise_fd_limit() {
			if new_limit < RECOMMENDED_OPEN_FILE_DESCRIPTOR_LIMIT {
				log::warn!(
					"⚠️  Low open file descriptor limit configured for the process. \
                     Current value: {:?}, recommended value: {:?}.",
					new_limit,
					RECOMMENDED_OPEN_FILE_DESCRIPTOR_LIMIT,
				);
			}
		}
		Ok(())
	}
}
