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

use super::cli_opts::CliOpts;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use substrate_archive::MigrationConfig;

#[derive(Clone)]
pub struct Config {
    db_path: PathBuf,
    psql_conf: MigrationConfig,
    cli: CliOpts,
    rpc_url: String,
    cache_size: usize,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TomlConfig {
    db_path: PathBuf,
    rpc_url: String,
    cache_size: usize,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
    db_host: Option<String>,
    db_port: Option<String>,
    db_user: Option<String>,
    db_pass: Option<String>,
    db_name: Option<String>,
}

impl Config {
    pub fn new() -> Result<Self> {
        let cli_opts = CliOpts::parse();
        let toml_conf = Self::parse_file(cli_opts.file.as_path())?;
        log::debug!("{:?}", toml_conf);

        let psql_conf = MigrationConfig {
            host: toml_conf.db_host.clone(),
            port: toml_conf.db_port.clone(),
            user: toml_conf.db_user.clone(),
            pass: toml_conf.db_pass.clone(),
            name: toml_conf.db_name.clone(),
        };

        Ok(Self {
            db_path: toml_conf.db_path,
            psql_conf,
            cli: cli_opts,
            rpc_url: toml_conf.rpc_url.clone(),
            cache_size: toml_conf.cache_size,
            block_workers: toml_conf.block_workers,
            wasm_pages: toml_conf.wasm_pages,
        })
    }

    fn parse_file(path: &Path) -> Result<TomlConfig> {
        let toml_str = std::fs::read_to_string(path)?;
        Ok(toml::from_str(toml_str.as_str())?)
    }

    pub fn cli(&self) -> &CliOpts {
        &self.cli
    }

    pub fn rpc_url(&self) -> &String {
        &self.rpc_url
    }

    pub fn cache_size(&self) -> usize {
        self.cache_size
    }

    pub fn psql_conf(&self) -> MigrationConfig {
        self.psql_conf.clone()
    }

    pub fn block_workers(&self) -> Option<usize> {
        self.block_workers
    }

    pub fn db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    pub fn wasm_pages(&self) -> Option<u64> {
        self.wasm_pages.clone()
    }
}
