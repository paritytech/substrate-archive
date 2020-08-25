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
use polkadot_service::{kusama_runtime::RuntimeApi, Block, KusamaExecutor};
use substrate_archive::{Archive, ArchiveConfig, MigrationConfig, ArchiveBuilder};

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Info);

    let conf = ArchiveConfig {
        db_url: "/home/insipx/.local/share/polkadot/chains/ksmcc3/db".into(),
        rpc_url: "ws://127.0.0.1:9944".into(),
        cache_size: 128,
        block_workers: Some(8),
        wasm_pages: None,
        psql_conf: MigrationConfig {
            host: None,
            port: None,
            user: Some("archive".to_string()),
            pass: Some("default".to_string()),
            name: Some("kusama-archive".to_string()),
        },
    };

    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let archive = ArchiveBuilder::<Block, RuntimeApi, KusamaExecutor>::new(conf, Box::new(spec)).unwrap();

    // start running the archive
    let context = rt.block_on(archive.run()).unwrap();

    // run indefinitely
    rt.block_on(context.block_until_stopped());
}
