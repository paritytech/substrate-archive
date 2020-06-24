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

use substrate_archive::{StorageKey, twox_128};
use anyhow::Result;
use std::path::{Path, PathBuf};
use serde::Deserialize;
use super::cli_opts::CliOpts;

#[derive(Clone)]
pub struct Config {
    keys: Vec<StorageKey>,
    db_path: PathBuf,
    psql_url: Option<String>, 
    cli: CliOpts,
}

#[derive(Debug, Clone)]
struct TomlConfig {
    keys: Vec<StorageKey>,
    db_path: PathBuf,
    psql_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TomlFile {
    db_path: PathBuf,
    psql_url: Option<String>,
    modules: Vec<Module> }

#[derive(Debug, Deserialize)]
struct Module {
    name: String,
    keys: Vec<String> 
}

impl Config {

    pub fn new() -> Result<Self>  {
        let cli_opts = CliOpts::parse();
        let toml_conf = Self::parse_file(cli_opts.file.as_path())?;
        log::debug!("{:?}", toml_conf);
        Ok(Self {
            keys: toml_conf.keys,
            db_path: toml_conf.db_path,
            psql_url: toml_conf.psql_url,
            cli: cli_opts
        })
    }

    fn parse_file(path: &Path) -> Result<TomlConfig> {
        let toml_str = std::fs::read_to_string(path)?;
        let decoded: TomlFile = toml::from_str(toml_str.as_str())?;

        let keys = decoded
            .modules
            .iter()
            .map(|m| Vec::<StorageKey>::from(m))
            .flatten()
            .collect::<Vec<StorageKey>>();
    
        Ok(TomlConfig {
            db_path: decoded.db_path,
            psql_url: decoded.psql_url,
            keys, 
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

    pub fn keys(&self) -> &[StorageKey] {
        self.keys.as_slice()
    }
}

impl From<&Module> for Vec<StorageKey> {
    fn from(module: &Module) -> Vec<StorageKey> {
        let mut key_vec = Vec::new();
        let master_key = twox_128(module.name.as_bytes()).to_vec();

        for key in module.keys.iter() {
            let key = twox_128(key.as_bytes()).to_vec();
            let mut full_key = master_key.clone();
            full_key.extend(key);
            key_vec.push(full_key);
        } 
        key_vec.into_iter().map(|k| StorageKey(k)).collect::<Vec<StorageKey>>()
    }
}
