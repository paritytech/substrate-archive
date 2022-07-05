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

use std::{borrow::Cow, fs, path::PathBuf};

use anyhow::Result;
use argh::FromArgs;

use substrate_archive::ArchiveConfig;

/// Specialized `ChainSpec`. This is a specialization of the general Substrate ChainSpec type.
pub type ChainSpec = sc_chain_spec::GenericChainSpec<node_template_runtime::GenesisConfig>;

/// Node Template Archiver for development use.
#[derive(Clone, FromArgs)]
pub struct CliOpts {
	/// sets a custom config file
	#[argh(option, short = 'c', long = "config")]
	pub config: Option<PathBuf>,
	/// sets spec for chain from a JSON file. Runs in `dev` mode by default.
	#[argh(option, default = "default_chain_spec()", short = 's', long = "spec", from_str_fn(parse_chain_spec))]
	pub chain_spec: ChainSpec,
}

fn parse_chain_spec(path: &str) -> Result<ChainSpec, String> {
	ChainSpec::from_json_file(PathBuf::from(path))
}

fn default_chain_spec() -> ChainSpec {
	let file = include_bytes!("./dev.json");
	let file: Cow<'static, [u8]> = Cow::Borrowed(file);
	ChainSpec::from_json_bytes(file).expect("Default ChainSpec `dev` could not be loaded")
}

impl CliOpts {
	pub fn init() -> Self {
		argh::from_env()
	}

	pub fn parse(&self) -> Result<Option<ArchiveConfig>> {
		if let Some(config) = &self.config {
			let toml_str = fs::read_to_string(config.as_path())?;
			let config = toml::from_str::<ArchiveConfig>(toml_str.as_str())?;
			Ok(Some(config))
		} else {
			Ok(None)
		}
	}
}
