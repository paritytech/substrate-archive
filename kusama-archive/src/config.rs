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

#[derive(Debug, Clone)]
pub struct Config {
    polkadot_path: PathBuf,
    psql_url: Option<String>,
    cli: CliOpts,
}

#[derive(Debug, Clone, Deserialize)]
struct TomlConfig {
    polkadot_path: PathBuf,
    db_host: Option<String>,
    db_port: Option<String>,
    db_user: Option<String>,
    db_pass: Option<String>,
    westend_db: Option<String>,
    kusama_db: Option<String>,
    polkadot_db: Option<String>,
}

impl Config {
    pub fn new() -> Result<Self> {
        let cli_opts = CliOpts::parse();
        let toml_conf = Self::parse_file(cli_opts.file.as_path())?;
        log::debug!("{:?}", toml_conf);
        Ok(Self {
            db_path: toml_conf.db_path,
            psql_url: toml_conf.psql_url,
            cli: cli_opts,
        })
    }

    fn parse_file(path: &Path) -> Result<TomlConfig> {
        let toml_str = std::fs::read_to_string(path)?;
        let decoded: TomlConfig = toml::from_str(toml_str.as_str())?;

        Ok(TomlConfig {
            db_path: decoded.db_path,
            psql_url: decoded.psql_url,
        })
    }

    pub fn cli(&self) -> &CliOpts {
        &self.cli
    }

    pub fn psql_url(&self) -> Option<&str> {
        self.psql_url.as_deref()
    }

    pub fn db_path(&self) -> &Path {
        self.db_path.as_path()
    }
}
