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

use crate::{
    actors::System,
    backend::{self, frontend::TArchiveClient, ReadOnlyBackend},
    error::Result,
    types,
};

use sc_chain_spec::ChainSpec;
use sc_client_api::backend as api_backend;
use sc_executor::NativeExecutionDispatch;
use serde::de::DeserializeOwned;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_blockchain::Backend as BlockchainBackend;
use sp_runtime::{
    generic::BlockId,
    traits::{BlakeTwo256, Block as BlockT, NumberFor},
};
use std::{marker::PhantomData, path::PathBuf, sync::Arc};

const CHAIN_DATA_VAR: &str = "CHAIN_DATA_DB";
const POSTGRES_VAR: &str = "DATABASE_URL";

/// The recommended open file descriptor limit to be configured for the process.
const RECOMMENDED_OPEN_FILE_DESCRIPTOR_LIMIT: u64 = 10_000;

pub struct Builder<B, R, D> {
    /// Path to the rocksdb database
    pub chain_data_path: Option<String>,
    /// url to the Postgres Database
    pub pg_url: Option<String>,
    /// how much Cache should Rocksdb Keep
    pub cache_size: Option<usize>,
    /// number of threads to spawn for block execution
    pub block_workers: Option<usize>,
    /// Number of 64KB Heap pages to allocate for wasm execution
    pub wasm_pages: Option<u64>,
    /// Chain spec describing the chain
    pub chain_spec: Option<Box<dyn ChainSpec>>,
    pub _marker: PhantomData<(B, R, D)>,
}

impl<B, R, D> Default for Builder<B, R, D> {
    fn default() -> Self {
        Self {
            chain_data_path: None,
            cache_size: None,
            pg_url: None,
            block_workers: None,
            wasm_pages: None,
            chain_spec: None,
            _marker: PhantomData,
        }
    }
}

impl<B, R, D> Builder<B, R, D> {
    /// Set the chain data backend path to use for this instance.
    ///
    /// # Default
    /// defaults to the environment variable CHAIN_DATA_DB
    pub fn chain_data_db<S: Into<String>>(mut self, path: S) -> Self {
        self.chain_data_path = Some(path.into());
        self
    }

    /// Set the url to the Postgres Database
    ///
    /// # Default
    /// defaults to value of the environment variable DATABASE_URL
    pub fn pg_url<S: Into<String>>(mut self, url: S) -> Self {
        self.pg_url = Some(url.into());
        self
    }

    /// Set the amount of cache Rocksdb should keep.
    ///
    /// # Default
    /// defaults to 128MB
    pub fn cache_size(mut self, cache_size: usize) -> Self {
        self.cache_size = Some(cache_size);
        self
    }

    /// Set the number of threads spawn for block execution.
    ///
    /// # Default
    /// defaults to the number of logical cpus in the system
    pub fn block_workers(mut self, workers: usize) -> Self {
        self.block_workers = Some(workers);
        self
    }

    /// Number of 64KB Heap Pages to allocate for WASM execution
    ///
    /// # Default
    /// defaults to 64 * (number of logic cpu's)
    pub fn wasm_pages(mut self, pages: u64) -> Self {
        self.wasm_pages = Some(pages);
        self
    }

    /// Specify a chain spec for storing metadata about the running archiver
    /// in a persistant directory.
    ///
    /// # Default
    /// Defaults to storing metadata in a temporary directory.
    pub fn chain_spec(mut self, spec: Box<dyn ChainSpec>) -> Self {
        self.chain_spec = Some(spec);
        self
    }
}

fn parse_urls(chain_data_path: Option<String>, pg_url: Option<String>) -> (String, String) {
    let chain_path = if let Some(path) = chain_data_path {
        path
    } else {
        std::env::var(CHAIN_DATA_VAR).expect("CHAIN_DATA_DB must be set if not passed initially.")
    };

    let pg_url = if let Some(url) = pg_url {
        url
    } else {
        std::env::var(POSTGRES_VAR).expect("DATABASE_URL must be set if not passed initially.")
    };

    (chain_path, pg_url)
}

/// Create rocksdb secondary directory if it doesn't exist yet.
/// If the ChainPpec is not specified, a temporary directory is used.
/// Return path to that directory
///
/// # Panics
///
/// Panics if the directories fail to be created.
fn create_database_path(spec: Option<Box<dyn ChainSpec>>) -> Result<PathBuf> {
    let path = if let Some(spec) = spec {
        let (chain, id) = (spec.name(), spec.id());
        let path = if let Some(base_dirs) = dirs::BaseDirs::new() {
            let mut path = base_dirs.data_local_dir().to_path_buf();
            path.push("substrate_archive");
            path.push("rocksdb_secondary");
            path.push(chain);
            path.push(id);
            path
        } else {
            panic!("Couldn't establish substrate data local path");
        };
        std::fs::create_dir_all(path.as_path())
            .expect("Unable to create rocksdb secondary directory");
        path
    } else {
        // TODO: make sure this is cleaned up on kill
        tempfile::tempdir()?.into_path()
    };

    Ok(path)
}

impl<B, R, D> Builder<B, R, D>
where
    B: BlockT + Unpin + DeserializeOwned,
    R: ConstructRuntimeApi<B, TArchiveClient<B, R, D>> + Send + Sync + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + sp_api::Metadata<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = api_backend::StateBackendFor<ReadOnlyBackend<B>, B>>
        + Send
        + Sync
        + 'static,
    D: NativeExecutionDispatch + 'static,
    <R::RuntimeApi as sp_api::ApiExt<B>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
    NumberFor<B>: Into<u32> + From<u32> + Unpin,
    B::Hash: From<primitive_types::H256> + Unpin,
    B::Header: serde::de::DeserializeOwned,
{
    /// Build this instance of the Archiver.
    /// Runs the database migrations for the database at `pg_url`.
    ///
    /// # Panics
    /// Panics if one of chain_data_db or pg_url is not passed to the builder
    /// and their respective environment variables are not set.
    pub fn build(self) -> Result<impl types::Archive<B>> {
        let num_cpus = num_cpus::get();
        let (chain_path, pg_url) = parse_urls(self.chain_data_path, self.pg_url);
        let cache_size = self.cache_size.unwrap_or(128);
        let block_workers = self.block_workers.unwrap_or(num_cpus);
        let wasm_pages = self.wasm_pages.unwrap_or(64 * num_cpus as u64);
        let db_path = create_database_path(self.chain_spec)?;
        smol::block_on(crate::migrations::migrate(&pg_url))?;
        let db = Arc::new(backend::util::open_database(
            chain_path.as_str(),
            cache_size,
            db_path,
        )?);
        let client = backend::runtime_api::<B, R, D>(db.clone(), block_workers, wasm_pages)?;
        let client = Arc::new(client);
        let backend = Arc::new(ReadOnlyBackend::new(db.clone(), true));
        Self::startup_info(&client, &backend)?;

        let ctx = System::<_, R, _>::new(client, backend, block_workers, pg_url.as_str())?;
        Ok(ctx)
    }

    /// Log some general startup info
    fn startup_info(client: &TArchiveClient<B, R, D>, backend: &ReadOnlyBackend<B>) -> Result<()> {
        let last_finalized_block = backend.last_finalized()?;
        let rt = client.runtime_version_at(&BlockId::Hash(last_finalized_block))?;
        log::info!(
            "Running archive for chain `{}` 🔗, implemention `{}`. Latest known runtime version: {}. Latest finalized block {} 🛡️",
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
