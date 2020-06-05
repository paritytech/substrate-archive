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
use substrate_archive::{backend, twox_128, Archive, ArchiveConfig, StorageKey, ArchiveContext};

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Warn, log::LevelFilter::Info);

    // create the keys we want to query storage for
    let system_key = twox_128(b"System").to_vec();
    let accounts_key = twox_128(b"Account").to_vec();
    let mut keys = Vec::new();

    let mut system_accounts = system_key.clone();
    system_accounts.extend(accounts_key);

    keys.push(StorageKey(system_accounts));

    let conf = ArchiveConfig {
        db_url: "/home/insipx/.local/share/polkadot/chains/ksmcc4/db".into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        psql_url: None,
        cache_size: 8192,
        keys,
    };

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();
    let archive = Archive::new(conf, spec).unwrap();
    let client = archive.client::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>().unwrap();
    let context = archive.run_with::<ksm_runtime::Runtime, _>(client).unwrap();

    // run indefinitely
    ArchiveContext::block_until_stopped().unwrap();
}
