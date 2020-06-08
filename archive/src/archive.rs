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
    backend::{self, ChainAccess, ApiAccess, RuntimeApiCollection},
    error::Error as ArchiveError,
    types::*,
};
use sc_client_db::Backend;
use sc_client_api::backend as api_backend;
use sc_executor::NativeExecutionDispatch;
use sc_service::{config::DatabaseConfig, ChainSpec, TFullBackend};
use sp_block_builder::BlockBuilder as BlockBuilderApi;
use sp_api::{ConstructRuntimeApi, ApiExt};
use sp_runtime::traits::{Block as BlockT, BlakeTwo256};
use sp_storage::StorageKey;
use std::{sync::Arc, marker::PhantomData};

pub struct Archive<Block: BlockT + Send + Sync, Spec: ChainSpec + Clone + 'static> {
    db_url: String,
    rpc_url: String,
    psql_url: Option<String>,
    keys: Vec<StorageKey>,
    cache_size: usize,
    spec: Spec,
    _marker: PhantomData<Block>
}

pub struct ArchiveConfig {
    pub db_url: String,
    pub rpc_url: String,
    pub psql_url: Option<String>,
    pub cache_size: usize,
    pub keys: Vec<StorageKey>
}

impl<Block, Spec> Archive<Block, Spec>
where
    Block: BlockT + Send + Sync,
   Spec: ChainSpec + Clone + 'static
{
    /// Create a new instance of the Archive DB
    pub fn new(conf: ArchiveConfig, spec: Spec) -> Result<Self, ArchiveError>
    {
        Ok(Self {
            spec,
            db_url: conf.db_url,
            rpc_url: conf.rpc_url,
            psql_url: conf.psql_url,
            cache_size: conf.cache_size,
            keys: conf.keys,
            _marker: PhantomData
        })
    }
    // pub fn client<Runtime, Dispatch>(&self) -> Result<
    /// create a new client
    pub fn client<Runtime, Dispatch>(
        &self,
    ) -> Result<Arc<impl ChainAccess<Block>>, ArchiveError>
    where
        Runtime: ConstructRuntimeApi<Block, sc_service::TFullClient<Block, Runtime, Dispatch>>
            + Send
            + Sync
            + 'static,
        Dispatch: NativeExecutionDispatch + 'static,
    {
        let db_config = self.make_db_conf()?;
        Ok(Arc::new(backend::client::<Block, Runtime, Dispatch, _>(db_config, self.spec.clone())
            .map_err(ArchiveError::from)?))
    }

    pub(crate) fn deref_client<Runtime, Dispatch>(
        &self,
    ) -> Result<impl ChainAccess<Block>, ArchiveError> 
    where
        Runtime: ConstructRuntimeApi<Block, sc_service::TFullClient<Block, Runtime, Dispatch>>
            + Send
            + Sync
            + 'static,
        Dispatch: NativeExecutionDispatch + 'static,
    {
        let db_config = self.make_db_conf()?;
        Ok(backend::client::<Block, Runtime, Dispatch, _>(db_config, self.spec.clone())
            .map_err(ArchiveError::from)?)
    }

    /// returns an Api Client with the associated backend
    pub fn api_client<Runtime, Dispatch>(
        &self,
    ) -> Result<impl ApiAccess<Block, TFullBackend<Block>, Runtime>, ArchiveError> 
    where
        Runtime: ConstructRuntimeApi<Block, sc_service::TFullClient<Block, Runtime, Dispatch>>
            + Send
            + Sync
            + 'static,
        Runtime::RuntimeApi: RuntimeApiCollection<
                Block, StateBackend = sc_client_api::StateBackendFor<TFullBackend<Block>, Block>,
            > + Send
            + Sync
            + 'static, 
        Dispatch: NativeExecutionDispatch + 'static,
        <Runtime::RuntimeApi as sp_api::ApiExt<Block>>::StateBackend: sp_api::StateBackend<BlakeTwo256>,
    {
        let db_config = self.make_db_conf()?;
        let (client, _) = backend::runtime_api::<Block, Runtime, Dispatch, _>(db_config, self.spec.clone())
                                    .map_err(ArchiveError::from)?;
        Ok(client)
    }

    /// Internal API to open the rocksdb database
    fn make_db_conf(&self) -> Result<DatabaseConfig, ArchiveError>
    {
        let db_path = std::path::PathBuf::from(self.db_url.as_str());
        let db_config = backend::open_database::<Block>(
            db_path.as_path(),
            self.cache_size,
            self.spec.name(),
            self.spec.id(),
        )?;
        Ok(db_config)
    }

    pub(crate) fn make_backend<T>(&self) -> Result<sc_client_db::Backend<NotSignedBlock<T>>, ArchiveError>
    where
        T: Substrate + Send + Sync,
        <T as System>::BlockNumber: Into<u32>,
        <T as System>::Hash: From<primitive_types::H256>,
        <T as System>::Header: serde::de::DeserializeOwned,
    {
        let settings = self.make_db_conf()?;
        let settings = sc_client_db::DatabaseSettings {
            state_cache_size: self.cache_size,
            state_cache_child_ratio: None,
            pruning: sc_client_db::PruningMode::ArchiveAll,
            source: settings,
        };
       sc_client_db::Backend::new(settings, 5).map_err(ArchiveError::from)
    }

    /// Returning context in which the archive is running
    pub fn run_with<T, Runtime, ClientApi, C>(&self, client: Arc<C>, client_api: Arc<ClientApi>) -> Result<ArchiveContext, ArchiveError>
    where
        T: Substrate + Send + Sync,
        C: ChainAccess<NotSignedBlock<T>> + 'static,
        Runtime: ConstructRuntimeApi<NotSignedBlock<T>, ClientApi>,
        Runtime::RuntimeApi: BlockBuilderApi<NotSignedBlock<T>, Error = sp_blockchain::Error>
            + ApiExt<NotSignedBlock<T>, StateBackend = api_backend::StateBackendFor<Backend<NotSignedBlock<T>>, NotSignedBlock<T>>>,
        ClientApi: ApiAccess<NotSignedBlock<T>, Backend<NotSignedBlock<T>>, Runtime> + 'static,
        <T as System>::BlockNumber: Into<u32>,
        <T as System>::Hash: From<primitive_types::H256>,
        <T as System>::Header: serde::de::DeserializeOwned,
    {
        let backend = self.make_backend::<T>()?;
        ArchiveContext::init::<T, Runtime, _, _>(
            client,
            client_api,
            backend,
            self.rpc_url.clone(),
            self.psql_url.as_deref()
        )
    }
}
