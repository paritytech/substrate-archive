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

use anyhow::{Context, Result};
use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use sc_chain_spec::ChainSpec;
use std::sync::Arc;
use substrate_archive::{Archive, ArchiveConfig};

pub fn run_archive(config: super::config::Config) -> Result<Archive<Block>> {
    let mut db_path = config.polkadot_path();

    let last_path_part = config
        .polkadot_path()
        .file_name()
        .context("Polkadot path not valid")?
        .to_str()
        .context("could not convert path to string")?;

    let spec_id: String = access_spec(|spec| spec.id().to_string());

    match last_path_part {
        "polkadot" => db_path.push(format!("chains/{}", spec_id)),
        "chains" => db_path.push(spec_id),
        _ => panic!(format!(
            "invalid polkadot path {}",
            config
                .polkadot_path()
                .as_path()
                .to_str()
                .context("Path not valid")?
        )
        .as_str()),
    }

    let db_path = db_path
        .as_path()
        .to_str()
        .context("could not convert rocksdb path to str")?
        .to_string();

    let conf = ArchiveConfig {
        db_url: db_path,
        rpc_url: config.rpc_url().into(),
        cache_size: config.cache_size(),
        psql_conf: config.psql_conf(),
    };

    access_spec(|spec| Ok(Archive::new(conf, spec)?))
}

fn access_spec<T, F, Spec>(fun: F, chain: &str) -> T
where
    F: FnOnce(&Spec) -> T,
    Spec: ChainSpec + Clone + 'static,
{
    match chain.to_ascii_lowercase() {
        "kusama" | "ksm" => {
            let spec = polkadot_service::chain_spec::kusama_config().unwrap();
            fun(spec)
        }
        "westend" => {
            let spec = polkadot_service::chain_spec::westend_config().unwrap();
            fun(spec)
        }
        "polkadot" | "dot" => {
            let spec = polkadot_service::chain_spec::polkadot_config().unwrap();
            fun(spec)
        }
    }
}
