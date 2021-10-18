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

use std::{fs, path::PathBuf, borrow::Cow};

use anyhow::Result;
use structopt::StructOpt;

use substrate_archive::ArchiveConfig;

/// Specialized `ChainSpec`. This is a specialization of the general Substrate ChainSpec type.
pub type ChainSpec = sc_chain_spec::GenericChainSpec<node_template_runtime::GenesisConfig>;

#[derive(Clone, StructOpt)]
#[structopt(author, about)]
pub struct CliOpts {
	/// Sets a custom config file
	#[structopt(short = "c", long, name = "FILE")]
	pub config: Option<PathBuf>,
	/// Sets spec for chain from a JSON file. Runs in `dev` mode by default.
	#[structopt(short = "s", long = "spec", name = "CHAIN", parse(from_str = parse_chain_spec))]
	pub chain_spec: ChainSpec,
}

impl Default for CliOpts {
	fn default() -> Self {

		let file = include_bytes!("./dev.json");
		let file: Cow<'static, [u8]> = Cow::Borrowed(file);
		let chain_spec: ChainSpec = ChainSpec::from_json_bytes(file).expect("Default ChainSpec `dev` could not be loaded");
		Self {
			config: None,
			chain_spec,
		}
	}
}

fn parse_chain_spec(path: &str) -> ChainSpec {
	ChainSpec::from_json_file(PathBuf::from(path)).expect("Chain spec could not be loaded")
}

impl CliOpts {
	pub fn init() -> Self {
		CliOpts::from_args()
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
