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
    actors::ArchiveContext,
    backend::{self, frontend::TArchiveClient, ApiAccess, ReadOnlyBackend, ReadOnlyDatabase},
    error::{ArchiveResult, Error as ArchiveError},
    migrations::MigrationConfig,
    rpc::Rpc,
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

pub struct Archive<Block, Runtime, Dispatch> {
    rpc_url: String,
    psql_url: String,
    db: Arc<ReadOnlyDatabase>,
    spec: Box<dyn ChainSpec>,
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

impl<B, R, D> Archive<B, R, D>
where
    B: BlockT,
    R: ConstructRuntimeApi<B, TArchiveClient<B, R, D>> + Send + Sync + 'static,
    R::RuntimeApi: BlockBuilderApi<B, Error = sp_blockchain::Error>
        + ApiExt<B, StateBackend = api_backend::StateBackendFor<ReadOnlyBackend<B>, B>>
        + Send
        + Sync
        + 'static,
    D: NativeExecutionDispatch + 'static,
    <R::RuntimeApi as sp_api::ApiExt<B>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
    NumberFor<B>: Into<u32>,
    NumberFor<B>: From<u32>,
    B::Hash: From<primitive_types::H256>,
    B::Header: serde::de::DeserializeOwned,
{
    /// Create a new instance of the Archive DB
    /// and run Postgres Migrations
    pub fn new(conf: ArchiveConfig, spec: Box<dyn ChainSpec>) -> Result<Self, ArchiveError> {
        let psql_url = crate::migrations::migrate(conf.psql_conf)?;
        let db = Arc::new(backend::util::open_database(
            conf.db_url.as_str(),
            conf.cache_size,
            spec.name(),
            spec.id(),
        )?);
        Ok(Self {
            db,
            psql_url,
            spec,
            rpc_url: conf.rpc_url,
            block_workers: conf.block_workers,
            wasm_pages: conf.wasm_pages,
            _marker: PhantomData,
        })
    }

    /// returns an Archive Client with a ReadOnlyBackend
    pub fn api_client(&self) -> Result<Arc<impl ApiAccess<B, ReadOnlyBackend<B>, R>>, ArchiveError> {
        let cpus = num_cpus::get();
        let backend = backend::runtime_api::<B, R, D>(
            self.db.clone(),
            self.block_workers.unwrap_or(cpus),
            self.wasm_pages.unwrap_or(2048),
        )
        .map_err(ArchiveError::from)?;
        Ok(Arc::new(backend))
    }

    /// Constructs the Archive and returns the context
    /// in which the archive is running.
    pub fn run(&self) -> Result<ArchiveContext<B>, ArchiveError> {
        let cpus = num_cpus::get();
        let client = backend::runtime_api::<B, R, D>(
            self.db.clone(),
            self.block_workers.unwrap_or(cpus),
            self.wasm_pages.unwrap_or(2048),
        )
        .map_err(ArchiveError::from)?;
        let client = Arc::new(client);
        let rt = client.runtime_version_at(&BlockId::Number(0.into()))?;
        self.verify_same_chain(rt)?;
        let backend = Arc::new(ReadOnlyBackend::new(self.db.clone(), true));
        ArchiveContext::init::<R, _>(
            client,
            backend,
            self.block_workers,
            self.rpc_url.clone(),
            self.psql_url.as_str(),
        )
    }

    /// Internal function to verify the running chain and the Runtime that was passed to us
    /// are the same
    fn verify_same_chain(&self, rt: RuntimeVersion) -> ArchiveResult<()> {
        let rpc = futures::executor::block_on(Rpc::<B>::connect(self.rpc_url.as_str()))?;
        let node_runtime = futures::executor::block_on(rpc.version(None))?;
        let (rpc_rstr, backend_rstr) = match (node_runtime.spec_name, rt.spec_name) {
            (RuntimeString::Borrowed(s0), RuntimeString::Borrowed(s1)) => (s0.to_string(), s1.to_string()),
            (RuntimeString::Owned(s0), RuntimeString::Owned(s1)) => (s0, s1),
            (RuntimeString::Borrowed(s0), RuntimeString::Owned(s1)) => (s0.to_string(), s1),
            (RuntimeString::Owned(s0), RuntimeString::Borrowed(s1)) => (s0, s1.to_string()),
        };
        if rpc_rstr.to_ascii_lowercase().as_str() != backend_rstr.to_ascii_lowercase().as_str() {
            return Err(ArchiveError::MismatchedChains(backend_rstr, rpc_rstr));
        } else {
            Ok(())
        }
    }
}
