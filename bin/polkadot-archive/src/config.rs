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
use serde::Deserialize;
use std::path::{Path, PathBuf};
use substrate_archive::MigrationConfig;

#[derive(Debug, Clone, Deserialize)]
struct TomlConfig {
    polkadot_path: PathBuf,
    cache_size: usize,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
    db_host: Option<String>,
    db_port: Option<String>,
    db_user: Option<String>,
    db_pass: Option<String>,
    westend_db: Option<String>,
    kusama_db: Option<String>,
    polkadot_db: Option<String>,
}

impl TomlConfig {
    pub fn migration_conf(&self, chain: &str) -> MigrationConfig {
        let name = match chain.to_ascii_lowercase().as_str() {
            "kusama" | "ksm" => self.kusama_db.clone(),
            "westend" => self.westend_db.clone(),
            "polkadot" | "dot" => self.polkadot_db.clone(),
            _ => panic!("Chain not specified"),
        };

        MigrationConfig {
            host: self.db_host.clone(),
            port: self.db_port.clone(),
            user: self.db_user.clone(),
            pass: self.db_pass.clone(),
            name: name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    polkadot_path: Option<PathBuf>,
    psql_conf: Option<MigrationConfig>,
    cli: CliOpts,
    cache_size: Option<usize>,
    block_workers: Option<usize>,
    wasm_pages: Option<u64>,
}

impl Config {
    pub fn new() -> Result<Self> {
        let cli_opts = CliOpts::parse();
        let toml_conf = cli_opts.clone().file.map(|f| {
            Self::parse_file(f.as_path())
        }).transpose()?;
        log::debug!("{:?}", toml_conf);
        
        Ok(Self {
            polkadot_path: toml_conf.as_ref().map(|p| p.polkadot_path.clone()),
            psql_conf: toml_conf.as_ref().map(|m| m.migration_conf(cli_opts.chain.as_str())),
            cli: cli_opts,
            cache_size: toml_conf.as_ref().map(|c| c.cache_size),
            block_workers: toml_conf.as_ref().map(|c| c.block_workers).flatten(),
            wasm_pages: toml_conf.as_ref().map(|c| c.wasm_pages).flatten(),
        })
    }

    fn parse_file(path: &Path) -> Result<TomlConfig> {
        let toml_str = std::fs::read_to_string(path)?;
        Ok(toml::from_str(toml_str.as_str())?)
    }

    pub fn cli(&self) -> &CliOpts {
        &self.cli
    }

    pub fn polkadot_path(&self) -> Option<PathBuf> {
        self.polkadot_path.clone()
    }

    pub fn cache_size(&self) -> Option<usize> {
        self.cache_size
    }

    pub fn psql_conf(&self) -> Option<MigrationConfig> {
        self.psql_conf.clone()
    }

    pub fn block_workers(&self) -> Option<usize> {
        self.block_workers
    }

    pub fn wasm_pages(&self) -> Option<u64> {
        self.wasm_pages
    }
}
