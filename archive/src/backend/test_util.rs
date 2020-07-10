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
    backend::{ApiAccess, ReadOnlyBackend},
    Archive, ArchiveConfig, MigrationConfig, System,
};
use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use sp_api::ProvideRuntimeApi;
use std::sync::Arc;

pub fn client(
    db: &str,
) -> Arc<impl ApiAccess<Block, ReadOnlyBackend<Block>, ksm_runtime::RuntimeApi>> {
    let conf = ArchiveConfig {
        db_url: db.into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        cache_size: 8192,
        psql_conf: MigrationConfig {
            host: None,
            port: None,
            user: Some("archive".to_string()),
            pass: Some("default".to_string()),
            name: Some("archive".to_string()),
        },
    };

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let archive = Archive::new(conf, spec).unwrap();
    let client = archive
        .api_client::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>()
        .unwrap();
    client
}

pub fn backend(db: &str) -> ReadOnlyBackend<Block> {
    let secondary_db = tempfile::Builder::new()
        .prefix("archive-test")
        .tempdir()
        .expect("Could not create temporary directory")
        .into_path();
    let conf = DatabaseConfig {
        secondary: Some(secondary_db.to_str().unwrap().to_string()),
        ..DatabaseConfig::with_columns(NUM_COLUMNS)
    };
    let db =
        Arc::new(ReadOnlyDatabase::open(&conf, db).expect("Couldn't open a secondary instance"));

    ReadOnlyBackend::new(db, true)
}

use crate::backend::database::ReadOnlyDatabase;
use crate::backend::util::NUM_COLUMNS;
use kvdb_rocksdb::DatabaseConfig;

pub fn harness<F>(db: &str, fun: F)
where
    F: FnOnce(ReadOnlyDatabase),
{
    let secondary_db = tempfile::Builder::new()
        .prefix("archive-test")
        .tempdir()
        .expect("could not create temporary directory")
        .into_path();
    let conf = DatabaseConfig {
        secondary: Some(secondary_db.to_str().unwrap().to_string()),
        ..DatabaseConfig::with_columns(NUM_COLUMNS)
    };
    let db = ReadOnlyDatabase::open(&conf, db).expect("Couldn't open a secondary instance");
    fun(db);
}
