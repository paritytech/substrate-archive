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
    backend::{self, frontend::TArchiveClient, ApiAccess, ReadOnlyBackend, ReadOnlyDatabase},
    error::{Error, Result},
    migrations::MigrationConfig,
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
use std::{marker::PhantomData, sync::Arc};

/// Main entrypoint for substrate-archive.
/// Deals with starting, stopping and manipulating the Actors
/// which drive the archive runtime.
///
/// # Examples
///
/// ```ignore
/// use polkadot_service::{kusama_runtime::RuntimeApi as RApi, Block, KusamaExecutor as KExec};
/// use substrate_archive::{Archive, ArchiveConfig, MigrationConfig};
/// let conf = ArchiveConfig {
///     db_url: "/home/insipx/.local/share/polkadot/chains/ksmcc3/db".into(),
///     rpc_url: "ws://127.0.0.1:9944".into(),
///     cache_size: 1024,
///     block_workers: None,
///     wasm_pages: None,
///     psql_conf: MigrationConfig {
///         host: None,
///         port: None,
///         user: Some("archive".to_string()),
///         pass: Some("default".to_string()),
///         name: Some("kusama-archive".to_string()),
///     },
/// };
///
/// let spec = polkadot_service::chain_spec::kusama_config().unwrap();
/// let archive = Archive::<Block, RApi, KExec>::new(conf, Box::new(spec)).unwrap();
/// let archive = archive.run().unwrap();
///
/// archive.block_until_stopped();
///
/// ```
pub struct ArchiveBuilder<Block, Runtime, Dispatch> {
    rpc_url: String,
    db: Arc<ReadOnlyDatabase>,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
    pg_url: String,
    _marker: PhantomData<(Block, Runtime, Dispatch)>,
}

pub struct ArchiveConfig {
    /// Path to the rocksdb database
    pub db_url: String,
    /// websockets URL to the full node
    pub rpc_url: String,
    /// how much cache should rocksdb keep
    pub cache_size: Option<usize>,
    // /// the Postgres database configuration
    // pub pg_conf: Option<String>,
    pub pg_url: Option<String>,
    /// number of threads to spawn for block execution
    pub block_workers: Option<usize>,
    /// Number of 64KB Heap pages to allocate for wasm execution
    pub wasm_pages: Option<u64>,
}

// TODO: Make ArchiveBuilder a real builder
impl<B, R, D> ArchiveBuilder<B, R, D>
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
    /// Create a new instance of the Archive DB
    /// and run Postgres Migrations
    /// # Panics
    /// Panics if pg_url is `None` and the variable `DATABASE_URL` does not exist
    /// in the environment
    pub fn new(conf: ArchiveConfig, spec: Box<dyn ChainSpec>) -> Result<Self> {
        let db = Arc::new(backend::util::open_database(
            conf.db_url.as_str(),
            conf.cache_size.unwrap_or(128),
            spec.name(),
            spec.id(),
        )?);

        let pg_url = if let Some(url) = conf.pg_url {
            url
        } else {
            std::env::var("DATABASE_URL")
                .expect("must have DATABASE_URL defined in the environment")
        };

        Ok(Self {
            db,
            pg_url,
            rpc_url: conf.rpc_url,
            block_workers: conf.block_workers,
            wasm_pages: conf.wasm_pages,
            _marker: PhantomData,
        })
    }

    /// Create a new Substrate Client with a ReadOnlyBackend
    pub fn api_client(
        &self,
        block_workers: Option<usize>,
        wasm_pages: Option<usize>,
    ) -> Result<Arc<impl ApiAccess<B, ReadOnlyBackend<B>, R>>> {
        let cpus = num_cpus::get();
        let client = backend::runtime_api::<B, R, D>(
            self.db.clone(),
            block_workers.unwrap_or(cpus),
            wasm_pages.map(|v| v as u64).unwrap_or(2048 as u64),
        )?;
        Ok(Arc::new(client))
    }

    /// Constructs the Archive and returns the context
    /// in which the archive is running.
    pub fn run(&self) -> Result<impl types::Archive<B>> {
        let ctx = smol::block_on(self._run())?;
        Ok(ctx)
    }

    async fn _run(&self) -> Result<impl types::Archive<B>> {
        let psql_url = crate::migrations::migrate(&self.pg_url).await?;
        let cpus = num_cpus::get();
        let block_workers = self.block_workers.unwrap_or(cpus);
        let client = backend::runtime_api::<B, R, D>(
            self.db.clone(),
            block_workers,
            self.wasm_pages.unwrap_or(512),
        )
        .map_err(Error::from)?;
        let client = Arc::new(client);
        let backend = Arc::new(ReadOnlyBackend::new(self.db.clone(), true));
        let last_finalized_block = backend.last_finalized()?;
        let rt = client.runtime_version_at(&BlockId::Hash(last_finalized_block))?;
        log::info!(
            "Running archive for chain `{}`, implemention `{}`. Latest known runtime version: {}. Latest finalized block {}",
            rt.spec_name,
            rt.impl_name,
            rt.spec_version,
            last_finalized_block
        );

        let ctx = System::<_, R, _>::new(
            client,
            backend,
            block_workers,
            self.rpc_url.clone(),
            psql_url.as_str(),
        )?;

        Ok(ctx)
    }
}
