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

use std::path::PathBuf;

use clap::{load_yaml, App};

#[derive(Clone)]
pub struct CliOpts {
	pub file: Option<PathBuf>,
	pub log_level: log::LevelFilter,
	pub chain_spec: node_template::chain_spec::ChainSpec,
}

impl CliOpts {
	pub fn parse() -> Self {
		let yaml = load_yaml!("cli_opts.yaml");
		let matches = App::from(yaml).get_matches();
		let file = matches.value_of("config");
		let log_level = match matches.occurrences_of("verbose") {
			0 => log::LevelFilter::Info,
			1 => log::LevelFilter::Info,
			2 => log::LevelFilter::Info,
			3 => log::LevelFilter::Debug,
			4 | _ => log::LevelFilter::Trace,
		};
		let chain_spec = match matches.value_of("spec") {
			Some("dev") => node_template::chain_spec::development_config(),
			Some("") | Some("local") => node_template::chain_spec::local_testnet_config(),
			Some(path) => node_template::chain_spec::ChainSpec::from_json_file(PathBuf::from(path)),
			_ => panic!("Chain spec could not be loaded; is the path correct?"),
		};
		CliOpts {
			file: file.map(PathBuf::from),
			log_level,
			chain_spec: chain_spec.expect("Chain spec could not be loaded"),
		}
	}
}
