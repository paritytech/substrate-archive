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
use polkadot_service::kusama_runtime as ksm_rt;
use polkadot_service::polkadot_runtime as dot_rt;
use polkadot_service::westend_runtime as westend_rt;
use polkadot_service::Block;
use sc_chain_spec::ChainSpec;
use substrate_archive::{Archive, ArchiveConfig, ArchiveContext};

pub enum TripleContext {
    Westend(ArchiveContext<Block>),
    Kusama(ArchiveContext<Block>),
    Polkadot(ArchiveContext<Block>),
}

impl TripleContext {
    pub fn shutdown(self) {
        match self {
            TripleContext::Westend(w) => w.shutdown().unwrap(),
            TripleContext::Kusama(k) => k.shutdown().unwrap(),
            TripleContext::Polkadot(p) => p.shutdown().unwrap(),
        }
    }
}

pub fn run_archive(config: Config) -> Result<TripleContext> {
    let mut db_path = config.polkadot_path();

    let path = config.polkadot_path();

    let last_path_part = path
        .file_name()
        .context("Polkadot path not valid")?
        .to_str()
        .context("could not convert path to string")?;

    let spec = get_spec(config.cli().chain.as_str())?;

    match last_path_part {
        "polkadot" => db_path.push(format!("chains/{}/db", spec.id())),
        "chains" => db_path.push(format!("{}/db", spec.id())),
        _ => return Err(anyhow!("invalid path {}", path.as_path().display())),
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
        block_workers: config.block_workers(),
        wasm_pages: config.wasm_pages(),
        psql_conf: config.psql_conf(),
    };

    match config.cli().chain.to_ascii_lowercase().as_str() {
        "kusama" | "ksm" => {
            let archive =
                Archive::<Block, ksm_rt::RuntimeApi, polkadot_service::KusamaExecutor>::new(
                    conf, spec,
                )?;
            Ok(TripleContext::Kusama(archive.run()?))
        }
        "westend" => {
            let archive =
                Archive::<Block, westend_rt::RuntimeApi, polkadot_service::WestendExecutor>::new(
                    conf, spec,
                )?;
            Ok(TripleContext::Westend(archive.run()?))
        }
        "polkadot" | "dot" => {
            let archive =
                Archive::<Block, dot_rt::RuntimeApi, polkadot_service::PolkadotExecutor>::new(
                    conf, spec,
                )?;
            Ok(TripleContext::Polkadot(archive.run()?))
        }
        c => Err(anyhow!("unknown chain {}", c)),
    }
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
