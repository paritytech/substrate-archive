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

use std::{fs, path::PathBuf};

use anyhow::Result;
use structopt::StructOpt;

use substrate_archive::ArchiveConfig;

#[derive(Clone, Debug, StructOpt)]
#[structopt(author, about)]
pub struct CliOpts {
	/// Sets a custom config file
	#[structopt(short = "c", long, name = "FILE")]
	pub config: Option<PathBuf>,
	/// The chain to run substrate-archive for. One of kusama, westend, polkadot.
	#[structopt(short = "s", long = "spec", name = "CHAIN", default_value = "polkadot")]
	pub chain_spec: String,
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
