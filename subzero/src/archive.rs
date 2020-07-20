// Copyright 2018-2019 Parity Technologies (UK) Ltd.
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

use anyhow::Result;
use node_template_runtime::{self as runtime, opaque::Block};
use std::sync::Arc;
use substrate_archive::{Archive, ArchiveConfig, ArchiveContext};

pub fn run_archive(config: super::config::Config) -> Result<ArchiveContext<runtime::Runtime>> {
    let spec = config.cli().chain_spec.clone();

    let conf = ArchiveConfig {
        db_url: config.db_path().to_str().unwrap().to_string(),
        rpc_url: config.rpc_url().into(),
        cache_size: config.cache_size(),
        block_workers: config.block_workers(),
        wasm_pages: config.wasm_pages(),
        psql_conf: config.psql_conf(),
    };

    let archive = Archive::new(conf, Box::new(spec))?;

    let api_client =
        archive.api_client::<runtime::RuntimeApi, node_template::service::Executor>()?;
    let archive = archive.run_with::<runtime::Runtime, runtime::RuntimeApi, _>(api_client)?;

    Ok(archive)
}
