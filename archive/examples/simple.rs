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

//! A simple example

use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use substrate_archive::{backend, twox_128, Archive, StorageKey};

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Warn, log::LevelFilter::Info);
    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();

    // Open a RocksDB Database for the node we want to index
    let path = std::path::PathBuf::from("/home/insipx/.local/share/polkadot/chains/ksmcc3/db");
    let db = backend::open_database::<Block>(path.as_path(), 8192, spec.name(), spec.id()).unwrap();

    let client =
        backend::client::<Block, ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor, _>(
            db, spec,
        )
        .unwrap();

    // create the keys we want to query storage for
    let system_key = twox_128(b"System").to_vec();
    let accounts_key = twox_128(b"Account").to_vec();
    let mut keys = Vec::new();

    let mut system_accounts = system_key.clone();
    system_accounts.extend(accounts_key);

    keys.push(StorageKey(system_accounts));

    // initialize the Archive runtime
    let archive = Archive::init::<ksm_runtime::Runtime, _>(
        client,
        "ws://127.0.0.1:9944".to_string(),
        keys.as_slice(),
        None,
    )
    .unwrap();

    // run indefinitely
    Archive::block_until_stopped().unwrap();
}
