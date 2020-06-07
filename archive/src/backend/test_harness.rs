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

use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use crate::{backend, twox_128, Archive, ArchiveConfig, StorageKey, ArchiveContext, backend::{ChainAccess, ApiAccess}};
use std::sync::Arc;
use sc_client_db::Backend;
use sp_api::ProvideRuntimeApi;
use sc_service::TFullBackend;

pub fn client_backend(db: &str) -> (impl ApiAccess<Block, TFullBackend<Block>, ksm_runtime::RuntimeApi>, Backend<Block>)
{
    let conf = ArchiveConfig {
        db_url: db.into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        psql_url: None,
        cache_size: 8192,
        keys: Vec::new(),
    };

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let archive = Archive::<Block, _>::new(conf, spec).unwrap();
    let (client, _) = archive.api_client_pair::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>().unwrap();
    let backend = archive.make_backend::<ksm_runtime::Runtime>().unwrap();
    (client, backend)
}
