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

#[derive(Debug, Clone)]
pub struct CliOpts {
    pub file: Option<PathBuf>,
    pub log_level: log::LevelFilter,
    pub log_num: u64,
    pub chain: String,
}

impl CliOpts {
    pub fn parse() -> Self {
        let yaml = load_yaml!("cli_opts.yaml");
        let matches = App::from(yaml).get_matches();
        let log_level = match matches.occurrences_of("verbose") {
            0 => log::LevelFilter::Info,
            1 => log::LevelFilter::Info,
            2 => log::LevelFilter::Info,
            3 => log::LevelFilter::Debug,
            4 | _ => log::LevelFilter::Trace,
        };
        let log_num = matches.occurrences_of("verbose");
        let file = matches.value_of("config");

        let chain = matches.value_of("chain").unwrap_or("polkadot");

        CliOpts {
            file: file.map(|f| PathBuf::from(f)),
            log_level,
            log_num,
            chain: chain.to_string(),
        }
    }
}
