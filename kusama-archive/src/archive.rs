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

use super::config::Config;

use anyhow::{anyhow, Context, Result};
use polkadot_service::{kusama_runtime as ksm_runtime, Block};
use sc_chain_spec::ChainSpec;
use std::sync::Arc;
use substrate_archive::{Archive, ArchiveConfig, ArchiveContext, Substrate};

pub fn run_archive(config: Config) -> Result<ArchiveContext<ksm_runtime::Runtime>> {
    let mut db_path = config.polkadot_path();

    let path = config.polkadot_path();

    let last_path_part = path
        .file_name()
        .context("Polkadot path not valid")?
        .to_str()
        .context("could not convert path to string")?;

    let spec = get_spec(config.cli().chain.as_str())?;

    match last_path_part {
        "polkadot" => db_path.push(format!("chains/{}", spec.id())),
        "chains" => db_path.push(spec.id()),
        _ => return Err(anyhow!("invalid path {}", path.as_path().to_str())),
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
    let archive = Archive::new(conf, spec)?;
    let client_api =
        archive.api_client::<ksm_runtime::RuntimeApi, polkadot_service::KusamaExecutor>()?;
    Ok(archive.run_with::<ksm_runtime::Runtime, ksm_runtime::RuntimeApi, _>(client_api)?)
}

fn get_spec(chain: &str) -> Result<Box<dyn ChainSpec>> {
    match chain.to_ascii_lowercase().as_str() {
        "kusama" | "ksm" => {
            let spec = polkadot_service::chain_spec::kusama_config().unwrap();
            Ok(Box::new(spec) as Box<dyn ChainSpec>)
        }
        "westend" => {
            let spec = polkadot_service::chain_spec::westend_config().unwrap();
            Ok(Box::new(spec) as Box<dyn ChainSpec>)
        }
        "polkadot" | "dot" => {
            let spec = polkadot_service::chain_spec::polkadot_config().unwrap();
            Ok(Box::new(spec) as Box<dyn ChainSpec>)
        }
        c => Err(anyhow!("unknown chain {}", c)),
    }
}
