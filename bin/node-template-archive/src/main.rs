// Copyright 2018-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive.  If not, see <http://www.gnu.org/licenses/>.

mod cli_opts;
mod config;

use anyhow::Result;
use node_template_runtime::{self as runtime, opaque::Block};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use substrate_archive::{Archive, ArchiveBuilder};

pub fn main() -> Result<()> {
    let config = config::Config::new()?;
    substrate_archive::init_logger(config.cli().log_level, log::LevelFilter::Debug);

    let archive = ArchiveBuilder::<Block, runtime::RuntimeApi, node_template::service::Executor> {
        block_workers: config.block_workers(),
        wasm_pages: config.wasm_pages(),
        cache_size: config.cache_size(),
        ..ArchiveBuilder::default()
    }
    .chain_data_db(config.db_path().to_str().unwrap().to_string())
    .pg_url(config.psql_conf().url())
    .chain_spec(Box::new(config.cli().chain_spec.clone()))
    .build()?;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");
    while running.load(Ordering::SeqCst) {}
    archive.shutdown()?;
    Ok(())
}
