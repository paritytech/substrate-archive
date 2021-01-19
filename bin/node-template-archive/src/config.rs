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

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use substrate_archive::MigrationConfig;

use crate::cli_opts::CliOpts;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TomlConfig {
	db_path: Option<PathBuf>,
	cache_size: Option<usize>,
	block_workers: Option<usize>,
	wasm_pages: Option<u64>,
	db_host: Option<String>,
	db_port: Option<String>,
	db_user: Option<String>,
	db_pass: Option<String>,
	db_name: Option<String>,
}

impl TomlConfig {
	fn migration_conf(&self) -> MigrationConfig {
		MigrationConfig {
			host: self.db_host.clone(),
			port: self.db_port.clone(),
			user: self.db_user.clone(),
			pass: self.db_pass.clone(),
			name: self.db_name.clone(),
		}
	}
}

#[derive(Clone)]
pub struct Config {
	cli: CliOpts,
	db_path: Option<PathBuf>,
	psql_conf: Option<MigrationConfig>,
	cache_size: Option<usize>,
	block_workers: Option<usize>,
	wasm_pages: Option<u64>,
}

impl Config {
	pub fn new() -> Result<Self> {
		let cli = CliOpts::parse();
		let toml_conf = cli.file.clone().map(|f| Self::parse_file(f.as_path())).transpose()?;
		log::debug!("{:?}", toml_conf);

		Ok(Self {
			cli,
			db_path: toml_conf.as_ref().and_then(|c| c.db_path.clone()),
			psql_conf: toml_conf.as_ref().and_then(|c| Some(c.migration_conf())),
			cache_size: toml_conf.as_ref().and_then(|c| c.cache_size),
			block_workers: toml_conf.as_ref().and_then(|c| c.block_workers),
			wasm_pages: toml_conf.as_ref().and_then(|c| c.wasm_pages),
		})
	}

	fn parse_file(path: &Path) -> Result<TomlConfig> {
		let toml_str = std::fs::read_to_string(path)?;
		Ok(toml::from_str(toml_str.as_str())?)
	}

	pub fn cli(&self) -> &CliOpts {
		&self.cli
	}

	pub fn db_path(&self) -> Option<PathBuf> {
		self.db_path.clone()
	}

	pub fn psql_conf(&self) -> Option<MigrationConfig> {
		self.psql_conf.clone()
	}

	pub fn cache_size(&self) -> Option<usize> {
		self.cache_size
	}

	pub fn block_workers(&self) -> Option<usize> {
		self.block_workers
	}

	pub fn wasm_pages(&self) -> Option<u64> {
		self.wasm_pages.clone()
	}
}
