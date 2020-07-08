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
    error::{ArchiveResult, Error as ArchiveError},
    migrations::MigrationConfig,
    rpc::Rpc,
    types,
};

use sc_chain_spec::ChainSpec;
use sc_client_api::backend as api_backend;
use sc_executor::NativeExecutionDispatch;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    generic::BlockId,
    traits::{BlakeTwo256, Block as BlockT, NumberFor},
    RuntimeString,
};
use sp_version::RuntimeVersion;
use std::{marker::PhantomData, sync::Arc};

/// Main entrypoint for substrate-archive.
/// Deals with starting, stopping and manipulating the Actors
/// which drive the archive runtime.
///
/// # Examples
///
/// ```
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
    psql_url: String,
    db: Arc<ReadOnlyDatabase>,
    // spec: Box<dyn ChainSpec>,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
    _marker: PhantomData<(Block, Runtime, Dispatch)>,
}

pub struct ArchiveConfig {
    /// Path to the rocksdb database
    pub db_url: String,
    /// websockets URL to the full node
    pub rpc_url: String,
    /// how much cache should rocksdb keep
    pub cache_size: usize,
    /// the Postgres database configuration
    pub psql_conf: MigrationConfig,
    /// number of threads to spawn for block execution
    pub block_workers: Option<usize>,
    /// Number of 64KB Heap pages to allocate for wasm execution
    pub wasm_pages: Option<u64>,
}

fn migrate(conf: MigrationConfig) -> Result<String, ArchiveError> {
    // refinery creates a current-thread tokio runtime that calls 'block_on', so we need to run possibly in its own thread
    // in case the user creates another runtime with tokio
    #[cfg(feature = "with-tokio")]
    {
        std::thread::spawn(move || crate::migrations::migrate(conf))
            .join()
            .expect("Migrations failed to run")
    }
    #[cfg(not(feature = "with-tokio"))]
    {
        crate::migrations::migrate(conf)
    }
}

impl<B, R, D> ArchiveBuilder<B, R, D>
where
    B: BlockT + Unpin,
    R: ConstructRuntimeApi<B, TArchiveClient<B, R, D>> + Send + Sync + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
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
    /// Should not be run within a futures runtime
    pub fn new(conf: ArchiveConfig, spec: Box<dyn ChainSpec>) -> Result<Self, ArchiveError> {
        let psql_url = migrate(conf.psql_conf.clone())?;

        let db = Arc::new(backend::util::open_database(
            conf.db_url.as_str(),
            conf.cache_size,
            spec.name(),
            spec.id(),
        )?);
        Ok(Self {
            db,
            psql_url,
            // spec,
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
    ) -> Result<Arc<impl ApiAccess<B, ReadOnlyBackend<B>, R>>, ArchiveError> {
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
    pub async fn run(&self) -> Result<impl types::Archive<B>, ArchiveError> {
        let cpus = num_cpus::get();
        log::info!("Creating client 0 ");
        let client0 = Arc::new(
            backend::runtime_api::<B, R, D>(
                self.db.clone(),
                self.block_workers.unwrap_or(cpus),
                self.wasm_pages.unwrap_or(512),
            )
            .map_err(ArchiveError::from)?,
        );
        log::info!("Creating client 1");
        let client1 = Arc::new(
            backend::runtime_api::<B, R, D>(self.db.clone(), 3, 64).map_err(ArchiveError::from)?,
        );

        let rt = client1.runtime_version_at(&BlockId::Number(0.into()))?;
        self.verify_same_chain(rt)?;
        let backend = Arc::new(ReadOnlyBackend::new(self.db.clone(), true));

        let mut ctx = System::<_, R, _>::new(
            (client0, client1),
            backend,
            self.block_workers,
            self.rpc_url.clone(),
            self.psql_url.as_str(),
        )?;
        ctx.drive().await?;
        Ok(ctx)
    }

    /// Internal function to verify the running chain and the Runtime that was passed to us
    /// are the same
    fn verify_same_chain(&self, rt: RuntimeVersion) -> ArchiveResult<()> {
        let rpc = futures::executor::block_on(Rpc::<B>::connect(self.rpc_url.as_str()))?;
        let node_runtime = futures::executor::block_on(rpc.version(None))?;
        let (rpc_rstr, backend_rstr) = match (node_runtime.spec_name, rt.spec_name) {
            (RuntimeString::Borrowed(s0), RuntimeString::Borrowed(s1)) => {
                (s0.to_string(), s1.to_string())
            }
            (RuntimeString::Owned(s0), RuntimeString::Owned(s1)) => (s0, s1),
            (RuntimeString::Borrowed(s0), RuntimeString::Owned(s1)) => (s0.to_string(), s1),
            (RuntimeString::Owned(s0), RuntimeString::Borrowed(s1)) => (s0, s1.to_string()),
        };
        if rpc_rstr.to_ascii_lowercase().as_str() != backend_rstr.to_ascii_lowercase().as_str() {
            Err(ArchiveError::MismatchedChains(backend_rstr, rpc_rstr))
        } else {
            Ok(())
        }
    }
}
