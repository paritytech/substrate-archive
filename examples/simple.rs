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
use substrate_archive::{Archive, ArchiveBuilder};

pub fn main() {
    substrate_archive::init_logger(log::LevelFilter::Info, log::LevelFilter::Info);
    
    // get spec/runtime from node library
    let spec = polkadot_service::chain_spec::kusama_config().unwrap();

    let db_url = std::env::var("DATABASE_URL").unwrap();
    let mut archive = ArchiveBuilder::<Block, RuntimeApi, KusamaExecutor>::default()
        .block_workers(2)
        .wasm_pages(512)
        .cache_size(128)
        .pg_url(db_url)
        .chain_spec(Box::new(spec))
        .build()
        .unwrap();

    archive.drive();
    archive.block_until_stopped();
}

