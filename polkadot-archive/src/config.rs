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
    rpc_url: String,
    cache_size: usize,
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
    polkadot_path: PathBuf,
    psql_conf: MigrationConfig,
    cli: CliOpts,
    rpc_url: String,
    cache_size: usize,
}

impl Config {
    pub fn new() -> Result<Self> {
        let cli_opts = CliOpts::parse();
        let toml_conf = Self::parse_file(cli_opts.file.as_path())?;
        log::debug!("{:?}", toml_conf);
        Ok(Self {
            polkadot_path: toml_conf.polkadot_path.clone(),
            psql_conf: toml_conf.migration_conf(cli_opts.chain.as_str()),
            cli: cli_opts,
            cache_size: toml_conf.cache_size,
            rpc_url: toml_conf.rpc_url.clone(),
        })
    }

    fn parse_file(path: &Path) -> Result<TomlConfig> {
        let toml_str = std::fs::read_to_string(path)?;
        Ok(toml::from_str(toml_str.as_str())?)
    }

    pub fn cli(&self) -> &CliOpts {
        &self.cli
    }

    pub fn polkadot_path(&self) -> PathBuf {
        self.polkadot_path.clone()
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
}
