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
    backend::{
        self, frontend::TArchiveClient, ApiAccess, ReadOnlyBackend, ReadOnlyDatabase,
        RuntimeApiCollection,
    },
    error::Error as ArchiveError,
    migrations::MigrationConfig,
    rpc::Rpc,
    types::*,
};
use sc_chain_spec::ChainSpec;
use sc_client_api::backend as api_backend;
use sc_executor::NativeExecutionDispatch;
use sp_api::{ApiExt, ConstructRuntimeApi};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_runtime::{
    traits::{BlakeTwo256, Block as BlockT},
    RuntimeString,
};
use std::{marker::PhantomData, sync::Arc};

pub struct Archive<Block: BlockT + Send + Sync> {
    rpc_url: String,
    psql_url: String,
    db: Arc<ReadOnlyDatabase>,
    spec: Box<dyn ChainSpec>,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
    _marker: PhantomData<Block>,
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

impl<Block> Archive<Block>
where
    Block: BlockT + Send + Sync,
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

    /// returns an Api Client with the associated backend
    pub fn api_client<Runtime, Dispatch>(
        &self,
    ) -> Result<Arc<impl ApiAccess<Block, ReadOnlyBackend<Block>, Runtime>>, ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, TArchiveClient<Block, Runtime, Dispatch>>
            + Send
            + Sync
            + 'static,
        Runtime::RuntimeApi: RuntimeApiCollection<
                Block,
                StateBackend = sc_client_api::StateBackendFor<ReadOnlyBackend<Block>, Block>,
            > + Send
            + Sync
            + 'static,
        Dispatch: NativeExecutionDispatch + 'static,
        <Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend:
            sp_api::StateBackend<BlakeTwo256>,
    {
        let cpus = num_cpus::get();
        let backend = backend::runtime_api::<Block, Runtime, Dispatch>(
            self.db.clone(),
            self.block_workers.unwrap_or(cpus),
            self.wasm_pages.unwrap_or(2048),
        )
        .map_err(ArchiveError::from)?;
        Ok(Arc::new(backend))
    }

    /// Constructs the Archive and returns the context
    /// in which the archive is running.
    pub fn run_with<T, Runtime, ClientApi>(
        &self,
        client_api: Arc<ClientApi>,
    ) -> Result<ArchiveContext<T>, ArchiveError>
    where
        T: Substrate + Send + Sync,
        Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi> + Send + 'static,
        Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error>
            + ApiExt<
                NotSignedBlock<T>,
                StateBackend = api_backend::StateBackendFor<
                    ReadOnlyBackend<NotSignedBlock<T>>,
                    NotSignedBlock<T>,
                >,
            >,
        ClientApi:
            ApiAccess<NotSignedBlock<T>, ReadOnlyBackend<NotSignedBlock<T>>, Runtime> + 'static,
        <T as System>::BlockNumber: Into<u32>,
        <T as System>::Hash: From<primitive_types::H256>,
        <T as System>::Header: serde::de::DeserializeOwned,
        <T as System>::BlockNumber: From<u32>,
    {
        self.verify_same_chain::<T>()?;
        let backend = Arc::new(ReadOnlyBackend::new(self.db.clone(), true));
        ArchiveContext::init(
            client_api,
            backend,
            self.block_workers,
            self.rpc_url.clone(),
            self.psql_url.as_str(),
        )
    }

    fn verify_same_chain<T>(&self) -> Result<(), ArchiveError>
    where
        T: Substrate + Send + Sync,
    {
        let rpc = futures::executor::block_on(Rpc::<T>::connect(self.rpc_url.as_str()))?;
        let node_runtime = futures::executor::block_on(rpc.version(None))?;
        let rstr = match node_runtime.spec_name {
            RuntimeString::Borrowed(s) => s.to_string(),
            RuntimeString::Owned(s) => s,
        };
        if rstr != self.spec.name().to_ascii_lowercase().as_str() {
            return Err(ArchiveError::MismatchedChains);
        } else {
            Ok(())
        }
    }
}
