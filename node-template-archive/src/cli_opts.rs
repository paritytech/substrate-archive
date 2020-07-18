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

use clap::{load_yaml, App};
use std::path::PathBuf;

#[derive(Clone)]
pub struct CliOpts {
    pub file: PathBuf,
    pub log_level: log::LevelFilter,
    pub chain_spec: node_template::chain_spec::ChainSpec,
}

impl CliOpts {
    pub fn parse() -> Self {
        let yaml = load_yaml!("cli_opts.yaml");
        let matches = App::from(yaml).get_matches();
        let log_level = match matches.occurrences_of("verbose") {
            0 => log::LevelFilter::Error,
            1 => log::LevelFilter::Warn,
            2 => log::LevelFilter::Info,
            3 => log::LevelFilter::Debug,
            4 | _ => log::LevelFilter::Trace,
        };
        let file = matches
            .value_of("config")
            .expect("Config is a required value");
        let chain_spec;
        let spec = matches.value_of("spec");
        if spec.is_some() {
            match spec {
                Some("dev") => {
                    chain_spec = node_template::chain_spec::development_config();
                }
                Some("") | Some("local") => {
                    chain_spec = node_template::chain_spec::local_testnet_config();
                }
                path => {
                    chain_spec = node_template::chain_spec::ChainSpec::from_json_file(
                        std::path::PathBuf::from(path.expect("checked for existence; qed")),
                    )
                    .expect("Couldn't load spec from file")
                }
            }
        } else {
            panic!("Chain spec could not be loaded; is the path correct?")
        }
        CliOpts {
            file: PathBuf::from(file),
            log_level,
            chain_spec,
        }
    }
}
