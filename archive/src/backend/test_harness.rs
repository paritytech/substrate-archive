// Copyright 2019-2020 Parity Technologies (UK) Ltd.
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
    backend,
    backend::{ApiAccess, ChainAccess},
    twox_128, Archive, ArchiveConfig, ArchiveContext, StorageKey,
};
use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use sc_client_db::Backend;
use sc_service::TFullBackend;
use sp_api::ProvideRuntimeApi;
use std::sync::Arc;

pub fn client_backend(
    db: &str,
) -> (
    Arc<impl ApiAccess<Block, TFullBackend<Block>, ksm_runtime::RuntimeApi>>,
    Arc<Backend<Block>>,
) {
    let conf = ArchiveConfig {
        db_url: db.into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        psql_url: None,
        cache_size: 8192,
    };

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let archive = Archive::new(conf, spec).unwrap();
    let client = archive
        .api_client::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>()
        .unwrap();
    let backend = archive.make_backend::<ksm_runtime::Runtime>().unwrap();
    let client = Arc::new(client);
    let backend = Arc::new(backend);
    (client, backend)
}

pub fn many_clients(
    db: &str,
    amount: usize,
) -> Vec<Arc<impl ApiAccess<Block, TFullBackend<Block>, ksm_runtime::RuntimeApi>>> {
    let conf = ArchiveConfig {
        db_url: db.into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        psql_url: None,
        cache_size: 2048,
    };

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let archive = Archive::new(conf, spec).unwrap();
    let mut clients = Vec::new();
    for i in 0..amount {
        let client = archive
            .api_client::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>()
            .unwrap();
        clients.push(Arc::new(client));
    }

    clients
}

pub fn client(db: &str) -> Arc<impl ChainAccess<Block>> {
    let conf = ArchiveConfig {
        db_url: db.into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        psql_url: None,
        cache_size: 8192,
    };
    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let archive = Archive::new(conf, spec).unwrap();
    let client = archive
        .client::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>()
        .unwrap();
    client
}
